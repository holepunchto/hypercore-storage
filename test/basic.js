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

test('delete tree node range', async function (t) {
  const c = await getCore(t)

  {
    let index = 10244242

    const b = c.createWriteBatch()

    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })
    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })
    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })
    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(BATCH, index++)
    const node2 = b.getTreeNode(BATCH, index++)
    const node3 = b.getTreeNode(BATCH, index++)
    const node4 = b.getTreeNode(BATCH, index++)
    b.tryFlush()

    t.alike(await node1, { index: 10244242, hash: HASH, size: 10 })
    t.alike(await node2, { index: 10244243, hash: HASH, size: 10 })
    t.alike(await node3, { index: 10244244, hash: HASH, size: 10 })
    t.alike(await node4, { index: 10244245, hash: HASH, size: 10 })
  }

  {
    const b = c.createWriteBatch()

    b.deleteTreeNodeRange(BATCH, 10244242, 10244246)

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(BATCH, index++)
    const node2 = b.getTreeNode(BATCH, index++)
    const node3 = b.getTreeNode(BATCH, index++)
    const node4 = b.getTreeNode(BATCH, index++)
    b.tryFlush()

    t.alike(await node1, null)
    t.alike(await node2, null)
    t.alike(await node3, null)
    t.alike(await node4, null)
  }
})

test('delete tree node range: no end', async function (t) {
  const c = await getCore(t)

  {
    let index = 10244242

    const b = c.createWriteBatch()

    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })
    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })
    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })
    b.addTreeNode(BATCH, { index: index++, hash: HASH, size: 10 })

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(BATCH, index++)
    const node2 = b.getTreeNode(BATCH, index++)
    const node3 = b.getTreeNode(BATCH, index++)
    const node4 = b.getTreeNode(BATCH, index++)
    b.tryFlush()

    t.alike(await node1, { index: 10244242, hash: HASH, size: 10 })
    t.alike(await node2, { index: 10244243, hash: HASH, size: 10 })
    t.alike(await node3, { index: 10244244, hash: HASH, size: 10 })
    t.alike(await node4, { index: 10244245, hash: HASH, size: 10 })
  }

  {
    const b = c.createWriteBatch()

    b.deleteTreeNodeRange(BATCH, 10244242)

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(BATCH, index++)
    const node2 = b.getTreeNode(BATCH, index++)
    const node3 = b.getTreeNode(BATCH, index++)
    const node4 = b.getTreeNode(BATCH, index++)
    b.tryFlush()

    t.alike(await node1, null)
    t.alike(await node2, null)
    t.alike(await node3, null)
    t.alike(await node4, null)
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

test('put blocks', async function (t) {
  const c = await getCore(t)

  const data = Buffer.alloc(32, 1)

  {
    const b = c.createWriteBatch()

    b.putBlock(BATCH, 10244242, data)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const has = b.hasBlock(BATCH, 10244242)
    const node = b.getBlock(BATCH, 10244242)
    const treeNode = b.getTreeNode(BATCH, 10244242)
    b.tryFlush()

    t.alike(await has, true)
    t.alike(await node, data)
    t.alike(await treeNode, null)
  }

  {
    const b = c.createWriteBatch()

    b.deleteBlock(BATCH, 10244242)

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node = b.getBlock(BATCH, 10244242)
    b.tryFlush()

    t.is(await node, null)
  }
})

test('delete block range', async function (t) {
  const c = await getCore(t)

  const data1 = Buffer.alloc(32, 1)
  const data2 = Buffer.alloc(32, 2)
  const data3 = Buffer.alloc(32, 3)
  const data4 = Buffer.alloc(32, 4)

  {
    const b = c.createWriteBatch()

    b.putBlock(BATCH, 10244242, data1)
    b.putBlock(BATCH, 10244243, data2)
    b.putBlock(BATCH, 10244244, data3)
    b.putBlock(BATCH, 10244245, data4)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(BATCH, 10244242)
    const node2 = b.getBlock(BATCH, 10244243)
    const node3 = b.getBlock(BATCH, 10244244)
    const node4 = b.getBlock(BATCH, 10244245)
    b.tryFlush()

    t.alike(await node1, data1)
    t.alike(await node2, data2)
    t.alike(await node3, data3)
    t.alike(await node4, data4)
  }

  {
    const b = c.createWriteBatch()

    b.deleteBlockRange(BATCH, 10244242, 10244246)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(BATCH, 10244242)
    const node2 = b.getBlock(BATCH, 10244243)
    const node3 = b.getBlock(BATCH, 10244244)
    const node4 = b.getBlock(BATCH, 10244245)
    b.tryFlush()

    t.alike(await node1, null)
    t.alike(await node2, null)
    t.alike(await node3, null)
    t.alike(await node4, null)
  }
})

test('delete block range: no end', async function (t) {
  const c = await getCore(t)

  const data1 = Buffer.alloc(32, 1)
  const data2 = Buffer.alloc(32, 2)
  const data3 = Buffer.alloc(32, 3)
  const data4 = Buffer.alloc(32, 4)

  {
    const b = c.createWriteBatch()

    b.putBlock(BATCH, 10244242, data1)
    b.putBlock(BATCH, 10244243, data2)
    b.putBlock(BATCH, 10244244, data3)
    b.putBlock(BATCH, 10244245, data4)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(BATCH, 10244242)
    const node2 = b.getBlock(BATCH, 10244243)
    const node3 = b.getBlock(BATCH, 10244244)
    const node4 = b.getBlock(BATCH, 10244245)
    b.tryFlush()

    t.alike(await node1, data1)
    t.alike(await node2, data2)
    t.alike(await node3, data3)
    t.alike(await node4, data4)
  }

  {
    const b = c.createWriteBatch()

    b.deleteBlockRange(BATCH, 10244242)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(BATCH, 10244242)
    const node2 = b.getBlock(BATCH, 10244243)
    const node3 = b.getBlock(BATCH, 10244244)
    const node4 = b.getBlock(BATCH, 10244245)
    b.tryFlush()

    t.alike(await node1, null)
    t.alike(await node2, null)
    t.alike(await node3, null)
    t.alike(await node4, null)
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
