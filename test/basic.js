const test = require('brittle')
const tmp = require('test-tmp')
const CoreStorage = require('../')

const DK_0 = Buffer.alloc(32).fill('dk0')
const HASH = Buffer.alloc(32).fill('hash')
const BATCH = 0

test('basic', async function (t) {
  const c = await getCore(t)

  {
    const b = c.createWriteBatch()

    b.addTreeNode(BATCH, {
      index: 42,
      hash: HASH,
      size: 10
    })

    b.addTreeNode(BATCH, {
      index: 43,
      hash: HASH,
      size: 2
    })

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node1 = b.getTreeNode(BATCH, 42)
    const node2 = b.getTreeNode(BATCH, 43)
    b.tryFlush()

    t.alike(await node1, { index: 42, hash: HASH, size: 10 })
    t.alike(await node2, { index: 43, hash: HASH, size: 2 })
  }
})

test('delete nodes', async function (t) {
  const c = await getCore(t)

  {
    const b = c.createWriteBatch()

    b.addTreeNode(BATCH, {
      index: 10244242,
      hash: HASH,
      size: 10
    })

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node = b.getTreeNode(BATCH, 10244242)
    b.tryFlush()

    t.alike(await node, { index: 10244242, hash: HASH, size: 10 })
  }

  {
    const b = c.createWriteBatch()

    b.deleteTreeNode(BATCH, 10244242)

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node = b.getTreeNode(BATCH, 10244242)
    b.tryFlush()

    t.is(await node, null)
  }
})

test('peek last tree node', async function (t) {
  const c = await getCore(t)

  {
    const b = c.createWriteBatch()

    b.addTreeNode(BATCH, {
      index: 10000000,
      hash: HASH,
      size: 10
    })

    b.addTreeNode(BATCH, {
      index: 1,
      hash: HASH,
      size: 10
    })

    b.addTreeNode(BATCH, {
      index: 10,
      hash: HASH,
      size: 10
    })

    await b.flush()
  }

  {
    const node = await c.peakLastTreeNode(BATCH)

    t.alike(await node, { index: 10000000, hash: HASH, size: 10 })
  }
})

async function getCore (t) {
  const dir = await tmp(t)

  const s = new CoreStorage(dir)
  const c = s.get(DK_0)

  t.teardown(() => s.close())

  t.is(await c.open(), false)
  await c.create({ key: DK_0 })

  return c
}
