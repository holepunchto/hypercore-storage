const test = require('brittle')
const tmp = require('test-tmp')
const CoreStorage = require('../')

const DK_0 = Buffer.alloc(32).fill('dk0')
const DK_1 = Buffer.alloc(32).fill('dk1')
const HASH = Buffer.alloc(32).fill('hash')

test('basic', async function (t) {
  const c = await getCore(t)

  {
    const b = c.createWriteBatch()

    b.putTreeNode({
      index: 42,
      hash: HASH,
      size: 10
    })

    b.putTreeNode({
      index: 43,
      hash: HASH,
      size: 2
    })

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node1 = b.getTreeNode(42)
    const node2 = b.getTreeNode(43)
    b.tryFlush()

    t.alike(await node1, { index: 42, hash: HASH, size: 10 })
    t.alike(await node2, { index: 43, hash: HASH, size: 2 })
  }
})

test('delete nodes', async function (t) {
  const c = await getCore(t)

  {
    const b = c.createWriteBatch()

    b.putTreeNode({
      index: 10244242,
      hash: HASH,
      size: 10
    })

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node = b.getTreeNode(10244242)
    b.tryFlush()

    t.alike(await node, { index: 10244242, hash: HASH, size: 10 })
  }

  {
    const b = c.createWriteBatch()

    b.deleteTreeNode(10244242)

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node = b.getTreeNode(10244242)
    b.tryFlush()

    t.is(await node, null)
  }
})

test('delete tree node range', async function (t) {
  const c = await getCore(t)

  {
    let index = 10244242

    const b = c.createWriteBatch()

    b.putTreeNode({ index: index++, hash: HASH, size: 10 })
    b.putTreeNode({ index: index++, hash: HASH, size: 10 })
    b.putTreeNode({ index: index++, hash: HASH, size: 10 })
    b.putTreeNode({ index: index++, hash: HASH, size: 10 })

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(index++)
    const node2 = b.getTreeNode(index++)
    const node3 = b.getTreeNode(index++)
    const node4 = b.getTreeNode(index++)
    b.tryFlush()

    t.alike(await node1, { index: 10244242, hash: HASH, size: 10 })
    t.alike(await node2, { index: 10244243, hash: HASH, size: 10 })
    t.alike(await node3, { index: 10244244, hash: HASH, size: 10 })
    t.alike(await node4, { index: 10244245, hash: HASH, size: 10 })
  }

  {
    const b = c.createWriteBatch()

    b.deleteTreeNodeRange(10244242, 10244246)

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(index++)
    const node2 = b.getTreeNode(index++)
    const node3 = b.getTreeNode(index++)
    const node4 = b.getTreeNode(index++)
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

    b.putTreeNode({ index: index++, hash: HASH, size: 10 })
    b.putTreeNode({ index: index++, hash: HASH, size: 10 })
    b.putTreeNode({ index: index++, hash: HASH, size: 10 })
    b.putTreeNode({ index: index++, hash: HASH, size: 10 })

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(index++)
    const node2 = b.getTreeNode(index++)
    const node3 = b.getTreeNode(index++)
    const node4 = b.getTreeNode(index++)
    b.tryFlush()

    t.alike(await node1, { index: 10244242, hash: HASH, size: 10 })
    t.alike(await node2, { index: 10244243, hash: HASH, size: 10 })
    t.alike(await node3, { index: 10244244, hash: HASH, size: 10 })
    t.alike(await node4, { index: 10244245, hash: HASH, size: 10 })
  }

  {
    const b = c.createWriteBatch()

    b.deleteTreeNodeRange(10244242, -1)

    await b.flush()
  }

  {
    let index = 10244242

    const b = c.createReadBatch()

    const node1 = b.getTreeNode(index++)
    const node2 = b.getTreeNode(index++)
    const node3 = b.getTreeNode(index++)
    const node4 = b.getTreeNode(index++)
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

    b.putTreeNode({
      index: 10000000,
      hash: HASH,
      size: 10
    })

    b.putTreeNode({
      index: 1,
      hash: HASH,
      size: 10
    })

    b.putTreeNode({
      index: 10,
      hash: HASH,
      size: 10
    })

    await b.flush()
  }

  {
    const node = await c.peakLastTreeNode()
    t.alike(await node, { index: 10000000, hash: HASH, size: 10 })
  }
})

test('put blocks', async function (t) {
  const c = await getCore(t)

  const data = Buffer.alloc(32, 1)

  {
    const b = c.createWriteBatch()

    b.putBlock(10244242, data)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const has = b.hasBlock(10244242)
    const node = b.getBlock(10244242)
    const treeNode = b.getTreeNode(10244242)
    b.tryFlush()

    t.alike(await has, true)
    t.alike(await node, data)
    t.alike(await treeNode, null)
  }

  {
    const b = c.createWriteBatch()

    b.deleteBlock(10244242)

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node = b.getBlock(10244242)
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

    b.putBlock(10244242, data1)
    b.putBlock(10244243, data2)
    b.putBlock(10244244, data3)
    b.putBlock(10244245, data4)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(10244242)
    const node2 = b.getBlock(10244243)
    const node3 = b.getBlock(10244244)
    const node4 = b.getBlock(10244245)
    b.tryFlush()

    t.alike(await node1, data1)
    t.alike(await node2, data2)
    t.alike(await node3, data3)
    t.alike(await node4, data4)
  }

  {
    const b = c.createWriteBatch()

    b.deleteBlockRange(10244242, 10244246)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(10244242)
    const node2 = b.getBlock(10244243)
    const node3 = b.getBlock(10244244)
    const node4 = b.getBlock(10244245)
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

    b.putBlock(10244242, data1)
    b.putBlock(10244243, data2)
    b.putBlock(10244244, data3)
    b.putBlock(10244245, data4)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(10244242)
    const node2 = b.getBlock(10244243)
    const node3 = b.getBlock(10244244)
    const node4 = b.getBlock(10244245)
    b.tryFlush()

    t.alike(await node1, data1)
    t.alike(await node2, data2)
    t.alike(await node3, data3)
    t.alike(await node4, data4)
  }

  {
    const b = c.createWriteBatch()

    b.deleteBlockRange(10244242, -1)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const node1 = b.getBlock(10244242)
    const node2 = b.getBlock(10244243)
    const node3 = b.getBlock(10244244)
    const node4 = b.getBlock(10244245)
    b.tryFlush()

    t.alike(await node1, null)
    t.alike(await node2, null)
    t.alike(await node3, null)
    t.alike(await node4, null)
  }
})

test('make two cores', async function (t) {
  const s = await getStorage(t)

  const c1 = s.get(DK_0)
  const c2 = s.get(DK_1)

  if (!(await c1.open())) await c1.create({ key: DK_0 })
  if (!(await c2.open())) await c2.create({ key: DK_1 })

  t.unlike(c1.corePointer, c2.corePointer)
})

test('bitfield pages', async function (t) {
  const c = await getCore(t)

  const empty = Buffer.alloc(4096)
  const full = Buffer.alloc(4096, 0xff)

  {
    const b = c.createWriteBatch()

    b.putBitfieldPage(0, empty)
    b.putBitfieldPage(1, full)
    b.putBitfieldPage(10244243, empty)
    b.putBitfieldPage(10244244, full)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const page1 = b.getBitfieldPage(0)
    const page2 = b.getBitfieldPage(1)
    const page3 = b.getBitfieldPage(10244243)
    const page4 = b.getBitfieldPage(10244244)
    const pageNull = b.getBitfieldPage(2)
    b.tryFlush()

    t.alike(await page1, empty)
    t.alike(await page2, full)
    t.alike(await page3, empty)
    t.alike(await page4, full)
    t.alike(await pageNull, null)
  }

  t.alike(await c.peakLastBitfieldPage(), { index: 10244244, page: full })

  {
    const pages = []
    for await (const page of c.createBitfieldPageStream()) {
      pages.push(page)
    }

    t.alike(pages, [
      { index: 0, page: empty },
      { index: 1, page: full },
      { index: 10244243, page: empty },
      { index: 10244244, page: full }
    ])
  }

  {
    const b = c.createWriteBatch()

    b.deleteBitfieldPage(0)
    b.deleteBitfieldPage(1)
    b.deleteBitfieldPage(10244243)
    b.deleteBitfieldPage(10244244)

    await b.flush()
  }

  {
    const b = c.createReadBatch()

    const page1 = b.getBitfieldPage(0)
    const page2 = b.getBitfieldPage(1)
    const page3 = b.getBitfieldPage(10244243)
    const page4 = b.getBitfieldPage(10244244)
    b.tryFlush()

    t.alike(await page1, null)
    t.alike(await page2, null)
    t.alike(await page3, null)
    t.alike(await page4, null)
  }

  t.alike(await c.peakLastBitfieldPage(), null)

  {
    const pages = []
    for await (const page of c.createBitfieldPageStream()) {
      pages.push(page)
    }

    t.alike(pages, [])
  }
})

test('make lots of cores in parallel', async function (t) {
  const s = await getStorage(t)
  const promises = []

  for (let i = 0; i < 1024; i++) {
    const key = Buffer.alloc(32).fill('#' + i)
    const c = s.get(key)
    promises.push(c.create({ key }))
  }

  await Promise.all(promises)

  const info = await s.info()
  t.is(info.total, 1024)
})

test('header', async function (t) {
  const dir = await tmp()

  const keyPair = {
    publicKey: Buffer.alloc(32, 0),
    secretKey: Buffer.alloc(32, 1)
  }

  const encryptionKey = Buffer.alloc(32, 2)

  const s1 = await getStorage(t, dir)
  const c1 = s1.get(DK_0)

  if (!(await c1.open())) await c1.create({ key: DK_0, keyPair, encryptionKey })

  const head = {
    fork: 1,
    length: 2,
    byteLength: 100099032,
    signature: Buffer.alloc(32, 4)
  }

  const w = c1.createWriteBatch()
  w.setCoreHead(head)
  await w.flush()

  await s1.close()

  const s2 = await getStorage(t, dir)
  const c2 = s2.get(DK_0)

  t.ok(await c2.open())

  const { key, manifest } = await c2.getCoreAuth()
  const kp = await c2.getLocalKeyPair()
  const enc = await c2.getEncryptionKey()

  t.alike(key, DK_0)
  t.alike(manifest, null)
  t.alike(kp, keyPair)
  t.alike(enc, encryptionKey)

  t.alike(await c2.getCoreHead(), head)
})

async function getStorage (t, dir) {
  if (!dir) dir = await tmp(t)
  const s = new CoreStorage(dir)

  t.teardown(() => s.close())

  return s
}

async function getCore (t) {
  const s = await getStorage(t)
  const c = s.get(DK_0)

  t.is(await c.open(), false)
  await c.create({ key: DK_0 })

  return c
}
