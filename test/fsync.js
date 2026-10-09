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
