const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

const Storage = require('../')

test('groups', async (t) => {
  const s = await create(t)

  let timestamp = 0
  const topic = b4a.alloc(32)
  const group = await s.createGroup(topic)

  t.is(group.pointer, 0)
  t.is(await s.getGroup(topic), group.pointer)

  const core = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 1)
  })

  const tx = core.write()
  tx.putGroupUpdate(group.pointer, timestamp++, b4a.alloc(32, 1))
  await tx.flush()

  {
    const values = []
    for await (const key of s.createGroupUpdateStream(group.pointer)) {
      values.push(key)
    }
    t.alike(values, [b4a.alloc(32, 1)])
  }

  {
    const values = []
    for await (const key of s.createGroupUpdateStream(group.pointer, { since: timestamp })) {
      values.push(key)
    }
    t.alike(values, [])
  }

  await s.close()
})

test('groups - multiple cores', async (t) => {
  const s = await create(t)

  let timestamp = 0
  const topic = b4a.alloc(32)
  const group = await s.createGroup(topic)

  const c1 = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 1)
  })

  const c2 = await s.createCore({
    key: b4a.alloc(32, 2),
    discoveryKey: b4a.alloc(32, 2)
  })

  const tx1 = c1.write()
  const tx2 = c2.write()

  tx2.putGroupUpdate(group.pointer, timestamp++, b4a.alloc(32, 2))
  tx1.putGroupUpdate(group.pointer, timestamp++, b4a.alloc(32, 1))

  await Promise.all([tx1.flush(), tx2.flush()])

  {
    const values = []
    for await (const key of s.createGroupUpdateStream(group.pointer)) {
      values.push(key)
    }
    t.alike(values, [b4a.alloc(32, 1), b4a.alloc(32, 2)])
  }

  {
    const values = []
    for await (const key of s.createGroupUpdateStream(group.pointer, { since: timestamp - 1 })) {
      values.push(key)
    }
    t.alike(values, [b4a.alloc(32, 1)])
  }

  await s.close()
})

test('groups - multiple groups', async (t) => {
  const s = await create(t)

  let timestamp = 0

  const group1 = await s.createGroup(b4a.alloc(32, 0))
  const group2 = await s.createGroup(b4a.alloc(32, 1))

  t.is(group1.pointer, 0)
  t.is(group2.pointer, 1)

  // group 1
  const c1 = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 1)
  })
  const c2 = await s.createCore({
    key: b4a.alloc(32, 2),
    discoveryKey: b4a.alloc(32, 2)
  })

  // group 2
  const c3 = await s.createCore({
    key: b4a.alloc(32, 3),
    discoveryKey: b4a.alloc(32, 3)
  })

  const tx1 = c1.write()
  const tx2 = c2.write()
  const tx3 = c3.write()

  tx2.putGroupUpdate(group1.pointer, timestamp++, b4a.alloc(32, 2))
  tx1.putGroupUpdate(group1.pointer, timestamp++, b4a.alloc(32, 1))
  tx3.putGroupUpdate(group2.pointer, timestamp++, b4a.alloc(32, 3))

  await Promise.all([tx1.flush(), tx2.flush(), tx3.flush()])

  {
    const values = []
    for await (const key of s.createGroupUpdateStream(group1.pointer)) {
      values.push(key)
    }
    t.alike(values, [b4a.alloc(32, 1), b4a.alloc(32, 2)])
  }

  {
    const values = []
    for await (const key of s.createGroupUpdateStream(group2.pointer)) {
      values.push(key)
    }
    t.alike(values, [b4a.alloc(32, 3)])
  }

  await s.close()
})

test('wakeup - persists', async (t) => {
  const dir = await t.tmp()

  let group = null
  {
    const s = new Storage(dir)

    let timestamp = 0
    const topic = b4a.alloc(32)
    group = await s.createGroup(topic)

    t.is(group.pointer, 0)

    const core = await s.createCore({
      key: b4a.alloc(32, 1),
      discoveryKey: b4a.alloc(32, 1)
    })

    const tx = core.write()
    tx.putGroupUpdate(group.pointer, timestamp++, b4a.alloc(32, 1))
    await tx.flush()

    await s.close()
  }

  {
    const s = new Storage(dir)
    const values = []

    const anotherGroup = await s.createGroup(b4a.alloc(32, 1))
    const origGroup = await s.createGroup(b4a.alloc(32))

    t.is(origGroup.pointer, 0)
    t.is(anotherGroup.pointer, 1)

    for await (const key of s.createGroupUpdateStream(group.pointer)) {
      values.push(key)
    }
    t.alike(values, [b4a.alloc(32, 1)])
    await s.close()
  }
})
