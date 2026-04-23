const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

const Storage = require('../')

test('wakeup', async (t) => {
  const s = await create(t)

  const topic = b4a.alloc(32)

  const wakeup = await s.createWakeupSession(topic)

  const updates = [
    { key: b4a.alloc(32, 1), length: 1 },
    { key: b4a.alloc(32, 1), length: 2 },
    { key: b4a.alloc(32, 2), length: 1 }
  ]

  await wakeup.addWakeup(updates[0].key, updates[0].length)
  await wakeup.addWakeup(updates[1].key, updates[1].length)
  await wakeup.addWakeup(updates[2].key, updates[2].length)

  t.alike(await wakeup.drain(), updates.slice(1))
  t.alike(await wakeup.drain(), [])

  await wakeup.close()
})

test('wakeup - concurrent', async (t) => {
  const s = await create(t)

  const topic = b4a.alloc(32)

  const wakeup = await s.createWakeupSession(topic)

  const updates = [
    { key: b4a.alloc(32, 1), length: 1 },
    { key: b4a.alloc(32, 1), length: 2 },
    { key: b4a.alloc(32, 2), length: 1 }
  ]

  const proms = []
  proms.push(wakeup.addWakeup(updates[0].key, updates[0].length))
  proms.push(wakeup.addWakeup(updates[1].key, updates[1].length))
  proms.push(wakeup.addWakeup(updates[2].key, updates[2].length))

  await Promise.all(proms)

  const drain = wakeup.drain()
  wakeup.addWakeup(b4a.alloc(32, 1), 3)

  t.alike(await drain, updates.slice(1))
  t.alike(await wakeup.drain(), [{ key: b4a.alloc(32, 1), length: 3 }])

  await wakeup.close()
})

test('wakeup - max size', async (t) => {
  const s = await create(t)

  const topic = b4a.alloc(32)

  const wakeup = await s.createWakeupSession(topic, { maxSize: 4 })

  const updates = [
    { key: b4a.alloc(32, 1), length: 1 },
    { key: b4a.alloc(32, 2), length: 1 },
    { key: b4a.alloc(32, 3), length: 1 },
    { key: b4a.alloc(32, 4), length: 1 },
    { key: b4a.alloc(32, 5), length: 1 },
    { key: b4a.alloc(32, 6), length: 1 }
  ]

  for (const { key, length } of updates) {
    await wakeup.addWakeup(key, length)
  }

  const hints = await wakeup.drain()

  t.is(hints.length, 4)
  t.alike(hints, updates.slice(2))

  await wakeup.close()
})

test('wakeup - multiple sessions', async (t) => {
  const s = await create(t)

  const a = await s.createWakeupSession(b4a.alloc(32, 1))
  const b = await s.createWakeupSession(b4a.alloc(32, 2))

  const au = [
    { key: b4a.alloc(32, 1), length: 1 },
    { key: b4a.alloc(32, 2), length: 1 },
    { key: b4a.alloc(32, 3), length: 1 }
  ]

  const bu = [
    { key: b4a.alloc(32, 4), length: 1 },
    { key: b4a.alloc(32, 5), length: 1 },
    { key: b4a.alloc(32, 6), length: 1 }
  ]

  const proms = []
  for (const { key, length } of au) {
    await a.addWakeup(key, length)
  }

  for (const { key, length } of bu) {
    await b.addWakeup(key, length)
  }

  t.alike(await a.drain(), au)
  t.alike(await b.drain(), bu)

  await a.close()
  await b.close()
})

test('wakeup - persists', async (t) => {
  const dir = await t.tmp()

  const topic = b4a.alloc(32, 1)
  const updates = [
    { key: b4a.alloc(32, 1), length: 1 },
    { key: b4a.alloc(32, 2), length: 1 },
    { key: b4a.alloc(32, 3), length: 1 },
    { key: b4a.alloc(32, 1), length: 2 },
    { key: b4a.alloc(32, 2), length: 2 },
    { key: b4a.alloc(32, 3), length: 2 }
  ]

  {
    const s = new Storage(dir)
    const wakeup = await s.createWakeupSession(topic)

    for (let i = 0; i < 3; i++) {
      const { key, length } = updates[i]
      await wakeup.addWakeup(key, length)
    }

    t.alike(await wakeup.drain(), updates.slice(0, 3))

    for (let i = 3; i < 6; i++) {
      const { key, length } = updates[i]
      await wakeup.addWakeup(key, length)
    }

    await wakeup.close()
    await s.close()
  }

  {
    const s = new Storage(dir)
    const wakeup = await s.createWakeupSession(topic)

    t.alike(await wakeup.drain(), updates.slice(3, 6))

    await wakeup.close()
    await s.close()
  }
})
