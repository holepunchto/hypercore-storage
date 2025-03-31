const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { createCore, toArray, create } = require('./helpers')

test('block stream', async function (t) {
  const core = await createCore(t)

  const tx = core.write()
  const expected = []

  for (let i = 0; i < 10; i++) {
    tx.putBlock(i, b4a.from([i]))
    expected.push({ index: i, value: b4a.from([i]) })
  }

  await tx.flush()

  const blocks = await toArray(core.createBlockStream({ gte: 0, lt: 10 }))

  t.alike(blocks, expected)
})

test('dependency stream', async function (t) {
  const core = await createCore(t)

  const expected = []
  for (let i = 0; i < 30; i++) expected.push({ index: i, value: b4a.from([i]) })

  const head = {
    fork: 0,
    length: 0,
    rootHash: b4a.alloc(32),
    signature: b4a.alloc(64)
  }

  await writeBlocks(core, head, 10)

  const sess1 = await core.createSession('first', null)

  await writeBlocks(sess1, head, 10)

  const sess2 = await sess1.createSession('second', null)

  await writeBlocks(sess2, head, 10)

  const blocks = await toArray(sess2.createBlockStream({ gte: 0, lt: 30 }))

  t.alike(blocks, expected)
})

test('dependency stream with limits', async function (t) {
  const core = await createCore(t)

  const expected = []
  for (let i = 5; i < 25; i++) expected.push({ index: i, value: b4a.from([i]) })

  const head = {
    fork: 0,
    length: 0,
    rootHash: b4a.alloc(32),
    signature: b4a.alloc(64)
  }

  await writeBlocks(core, head, 10)

  const sess1 = await core.createSession('first', null)

  await writeBlocks(sess1, head, 10)

  const sess2 = await sess1.createSession('second', null)

  await writeBlocks(sess2, head, 10)

  const blocks = await toArray(sess2.createBlockStream({ gte: 5, lt: 25 }))

  t.alike(blocks, expected)
})

test('reverse block stream', async function (t) {
  const core = await createCore(t)

  const tx = core.write()
  const expected = []

  for (let i = 0; i < 10; i++) {
    tx.putBlock(i, b4a.from([i]))
    expected.push({ index: i, value: b4a.from([i]) })
  }

  await tx.flush()

  const blocks = await toArray(core.createBlockStream({ gte: 0, lt: 10, reverse: true }))

  t.alike(blocks, expected.reverse())
})

test('reverse dependency stream', async function (t) {
  const core = await createCore(t)

  const expected = []
  for (let i = 29; i >= 0; i--) expected.push({ index: i, value: b4a.from([i]) })

  const head = {
    fork: 0,
    length: 0,
    rootHash: b4a.alloc(32),
    signature: b4a.alloc(64)
  }

  await writeBlocks(core, head, 10)

  const sess1 = await core.createSession('first', null)

  await writeBlocks(sess1, head, 10)

  const sess2 = await sess1.createSession('second', null)

  await writeBlocks(sess2, head, 10)

  const blocks = await toArray(sess2.createBlockStream({ gte: 0, lt: 30, reverse: true }))

  t.alike(blocks, expected)
})

test('reverse dependency stream with limits', async function (t) {
  const core = await createCore(t)

  const expected = []
  for (let i = 24; i >= 5; i--) expected.push({ index: i, value: b4a.from([i]) })

  const head = {
    fork: 0,
    length: 0,
    rootHash: b4a.alloc(32),
    signature: b4a.alloc(64)
  }

  await writeBlocks(core, head, 10)

  const sess1 = await core.createSession('first', null)

  await writeBlocks(sess1, head, 10)

  const sess2 = await sess1.createSession('second', null)

  await writeBlocks(sess2, head, 10)

  const blocks = await toArray(sess2.createBlockStream({ gte: 5, lt: 25, reverse: true }))

  t.alike(blocks, expected)
})

test('block stream (atom)', async function (t) {
  const core = await createCore(t)
  const atom = core.createAtom()

  const a = core.atomize(atom)

  const expected = []

  {
    const tx = a.write()

    for (let i = 0; i < 5; i++) {
      const index = 2 * i
      tx.putBlock(index, b4a.from([index]))
      expected.push({ index, value: b4a.from([index]) })
    }

    await tx.flush()
  }

  await atom.flush()

  {
    const tx = a.write()

    for (let i = 0; i < 5; i++) {
      const index = 2 * i + 1
      tx.putBlock(index, b4a.from([index]))
      expected.push({ index, value: b4a.from([index]) })
    }

    await tx.flush()
  }

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10 }))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10, reverse: true }))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  {
    const tx = a.write()
    tx.deleteBlockRange(4, 6)
    await tx.flush()
  }

  expected.sort(cmpBlock).splice(4, 2)

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10 }))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10, reverse: true }))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  {
    const tx = a.write()
    tx.deleteBlockRange(0, 2)
    tx.deleteBlockRange(8, 9)
    await tx.flush()
  }

  expected.sort(cmpBlock)

  expected.shift()
  expected.shift()
  const tmp = expected.pop()
  expected.pop()
  expected.push(tmp)

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10 }))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10, reverse: true }))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  {
    const tx = a.write()
    tx.deleteBlockRange(8, 10)
    await tx.flush()
  }

  expected.sort(cmpBlock)
  expected.pop()

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10 }))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10, reverse: true }))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  t.ok(a.view.changes, 'used atomic view')

  await atom.flush()

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10 }))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream({ gte: 0, lt: 10, reverse: true }))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }
})

test('discoveryKey stream', async function (t) {
  const s = await create(t)
  const expected = []

  t.teardown(async function () {
    await s.close()
  })

  t.comment('All cores')
  for (let i = 0; i < 5; i++) {
    const discoveryKey = crypto.randomBytes(32)
    await s.create({ key: crypto.randomBytes(32), discoveryKey })

    expected.push(discoveryKey)
  }

  const discoveryKeys = await toArray(s.createDiscoveryKeyStream())

  t.alike(discoveryKeys.slice().sort((a, b) => Buffer.compare(a, b)),
    expected.slice().sort((a, b) => Buffer.compare(a, b)))
})

function cmpBlock (a, b) {
  return a.index - b.index
}

async function writeBlocks (sess, head, n) {
  const start = head.length

  const tx = sess.write()
  for (let i = start; i < start + n; i++) tx.putBlock(i, b4a.from([i]))

  head.length += n
  tx.setHead(head)

  await tx.flush()
}
