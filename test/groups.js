const test = require('brittle')
const b4a = require('b4a')
const { create, getGroup } = require('./helpers')

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

test('groups - core group is keyed by the core pointer', async (t) => {
  const s = await create(t)

  const group = await s.createGroup(b4a.alloc(32, 0))

  // drift the core and data counters apart, sessions allocate a data pointer
  // without allocating a core
  const drifter = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 1)
  })
  await drifter.createSession('drift', null)

  const core = await s.createCore({
    key: b4a.alloc(32, 2),
    discoveryKey: b4a.alloc(32, 2)
  })

  const other = await s.createCore({
    key: b4a.alloc(32, 3),
    discoveryKey: b4a.alloc(32, 3)
  })

  // precondition, the group of "core" must not be stored in "other"s slot
  t.not(core.core.corePointer, core.core.dataPointer)
  t.is(core.core.dataPointer, other.core.corePointer)

  const tx = core.write()
  tx.setGroup(group)
  await tx.flush()

  t.alike(await getGroup(core), group, 'group is readable on the core that set it')
  t.is(await getGroup(other), null, 'group did not leak into another core')
  t.is(await getGroup(drifter), null, 'group did not leak into another core')

  await s.close()
})

test('groups - core group persists', async (t) => {
  const dir = await t.tmp()

  const group = { key: b4a.alloc(32, 0), pointer: 0 }
  const discoveryKey = b4a.alloc(32, 2)

  {
    const s = new Storage(dir)

    t.alike(await s.createGroup(group.key), group)

    const drifter = await s.createCore({
      key: b4a.alloc(32, 1),
      discoveryKey: b4a.alloc(32, 1)
    })
    await drifter.createSession('drift', null)

    const core = await s.createCore({ key: b4a.alloc(32, 2), discoveryKey })

    const tx = core.write()
    tx.setGroup(group)
    await tx.flush()

    await s.close()
  }

  {
    const s = new Storage(dir)

    const core = await s.resumeCore(discoveryKey)
    t.alike(await getGroup(core), group)

    const other = await s.createCore({
      key: b4a.alloc(32, 3),
      discoveryKey: b4a.alloc(32, 3)
    })

    t.is(core.core.dataPointer, other.core.corePointer)
    t.is(await getGroup(other), null, 'group did not leak into another core')

    await s.close()
  }
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
