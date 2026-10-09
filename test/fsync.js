const test = require('brittle')
const b4a = require('b4a')
const Storage = require('../')
const { create } = require('./helpers')

test('fsync', async function (t) {
  const s = await create(t)
  await s.ready()

  const a = await s.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })

  const target = a.store.fsyncsStarted + 1

  t.is(s.fsyncs, 0)
  t.is(target, 1)

  await s.flushFsync(target)

  t.is(s.fsyncs, target)

  await a.close()
  await s.close()
})

test('fsync is debounced', async function (t) {
  const dir = await t.tmp()

  const s = new Storage(dir)
  await s.ready()

  const a = await s.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })
  const b = await s.createCore({ key: b4a.alloc(32, 2), discoveryKey: b4a.alloc(32, 2) })
  const c = await s.createCore({ key: b4a.alloc(32, 3), discoveryKey: b4a.alloc(32, 3) })

  const proms = [
    a.fsync(),
    a.fsync(),
    a.fsync(),
    b.fsync(),
    b.fsync(),
    b.fsync(),
    c.fsync(),
    c.fsync(),
    c.fsync()
  ]

  await Promise.all(proms)

  // debounced
  t.is(s.fsyncs, 2)

  await a.fsync()

  t.is(s.fsyncs, 3)

  await a.close()
  await s.close()
})

test('fsync needed is tracked per core and pruned on completion', async function (t) {
  const s = await create(t)
  await s.ready()

  const a = await s.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })
  const b = await s.createCore({ key: b4a.alloc(32, 2), discoveryKey: b4a.alloc(32, 2) })

  t.is(a.fsyncsNeeded(), 1)
  t.is(b.fsyncsNeeded(), 1)

  t.is(a.markFsync(), 1)
  t.is(a.fsyncsNeeded(), 1)
  t.is(s.needed.size, 1)

  await s.flushFsync(1)

  t.is(s.fsyncs, 1)
  t.is(s.needed.size, 0)
  t.is(a.fsyncsNeeded(), 1)

  t.is(b.markFsync(), 2)
  t.is(b.fsyncsNeeded(), 2)

  s.lastFsyncAt = 0
  const fsync = s.flushFsync(2)

  t.is(s.fsyncsStarted, 2)
  t.is(a.markFsync(), 3)

  await fsync

  t.is(s.fsyncs, 2)
  t.is(s.needed.size, 1)
  t.is(a.fsyncsNeeded(), 3)
  t.is(b.fsyncsNeeded(), 1)

  const reopened = await s.resumeCore(b4a.alloc(32, 1))
  t.is(reopened.fsyncsNeeded(), 3)

  await s.flushFsync(3)
  t.is(s.needed.size, 0)

  await reopened.close()
  await a.close()
  await b.close()
  await s.close()
})

test('fsync is triggered when the needed map hits its size limit', async function (t) {
  const s = await create(t)
  await s.ready()

  const cores = []
  for (let i = 0; i < 4096; i++) {
    const key = b4a.alloc(32)
    key.writeUInt32BE(i)
    cores.push(await s.createCore({ key, discoveryKey: key }))
  }

  for (let i = 0; i < 4095; i++) cores[i].markFsync()

  t.is(s.needed.size, 4095)
  t.is(s.fsyncing, null)

  cores[4095].markFsync()

  t.is(s.needed.size, 4096)
  t.not(s.fsyncing, null)

  await s.fsyncing.promise

  t.is(s.fsyncs, 1)
  t.is(s.needed.size, 0)

  for (const c of cores) await c.close()
  await s.close()
})
