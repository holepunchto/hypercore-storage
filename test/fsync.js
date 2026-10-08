const test = require('brittle')
const b4a = require('b4a')
const Storage = require('../')
const { create } = require('./helpers')

test('fsyncs', async function (t) {
  const s = await create(t)
  await s.ready()

  const a = await s.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })

  const target = a.fsyncsNeeded()

  t.is(s.fsyncs, 0)
  t.is(target, 1)

  await s.flushFsync(target)

  t.is(s.fsyncs, target)

  await a.close()
  await s.close()
})

test('fsyncs is persisted', async function (t) {
  const dir = await t.tmp()

  const s1 = new Storage(dir)
  await s1.ready()

  await s1.flushFsync(3)

  t.is(s1.fsyncs, 3)

  await s1.close()

  const s2 = new Storage(dir)
  await s2.ready()

  t.is(s2.fsyncs, 3)

  const a = await s2.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })

  const target = a.fsyncsNeeded()
  t.is(target, 4)

  await s2.flushFsync(target)

  t.is(s2.fsyncs, target)

  await a.close()
  await s2.close()
})

test('fsyncs is debounced', async function (t) {
  const dir = await t.tmp()

  const s = new Storage(dir)
  await s.ready()

  const a = await s.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })
  const b = await s.createCore({ key: b4a.alloc(32, 2), discoveryKey: b4a.alloc(32, 2) })
  const c = await s.createCore({ key: b4a.alloc(32, 3), discoveryKey: b4a.alloc(32, 3) })

  const t0 = a.fsyncsNeeded()
  const tb = b.fsyncsNeeded()
  const tc = c.fsyncsNeeded()

  t.is(t0, tb)
  t.is(t0, tc)

  const proms = []
  for (let i = 0; i < 10; i++) {
    proms.push(s.flushFsync(t0))
  }

  await Promise.all(proms)

  const t1 = a.fsyncsNeeded()

  // debounced
  t.is(s.fsyncs, t0)
  t.is(t1, s.fsyncs + 1)

  await a.close()
  await s.close()
})
