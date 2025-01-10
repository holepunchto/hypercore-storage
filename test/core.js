const test = require('brittle')
const b4a = require('b4a')
const { createCore, create } = require('./helpers')

test('read and write hypercore blocks', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const rx = core.read()
  const proms = [rx.getBlock(0), rx.getBlock(1), rx.getBlock(2)]
  rx.tryFlush()
  const res = await Promise.all(proms)
  t.is(b4a.toString(res[0]), 'block0')
  t.is(b4a.toString(res[1]), 'block1')
  t.is(res[2], null)
})

test('read and write hypercore blocks across multiple cores', async (t) => {
  const storage = await create(t)
  const keys0 = {
    key: b4a.from('0'.repeat(64), 'hex'),
    discoveryKey: b4a.from('a'.repeat(64), 'hex')
  }
  const keys1 = {
    key: b4a.from('1'.repeat(64), 'hex'),
    discoveryKey: b4a.from('b'.repeat(64), 'hex')
  }
  const keys2 = {
    key: b4a.from('2'.repeat(64), 'hex'),
    discoveryKey: b4a.from('c'.repeat(64), 'hex')
  }

  const [core0, core1, core2] = await Promise.all([
    storage.create(keys0),
    storage.create(keys1),
    storage.create(keys2)
  ])

  await Promise.all([
    writeBlocks(core0, 2, { pre: 'core0-' }),
    writeBlocks(core1, 2, { pre: 'core1-' }),
    writeBlocks(core2, 2, { pre: 'core2-' })
  ])

  const rx0 = core0.read()
  const rx1 = core1.read()
  const rx2 = core2.read()
  const p = Promise.all([
    rx0.getBlock(0),
    rx0.getBlock(1),
    rx1.getBlock(0),
    rx1.getBlock(1),
    rx2.getBlock(0),
    rx2.getBlock(1)
  ])
  rx0.tryFlush()
  rx1.tryFlush()
  rx2.tryFlush()

  const [c0Block0, c0Block1, c1Block0, c1Block1, c2Block0, c2Block1] = await p
  t.is(b4a.toString(c0Block0), 'core0-block0')
  t.is(b4a.toString(c0Block1), 'core0-block1')
  t.is(b4a.toString(c1Block0), 'core1-block0')
  t.is(b4a.toString(c1Block1), 'core1-block1')
  t.is(b4a.toString(c2Block0), 'core2-block0')
  t.is(b4a.toString(c2Block1), 'core2-block1')

  await Promise.all([core0.close(), core1.close(), core2.close()])
  await storage.close()
})

test('read and write hypercore blocks from snapshot', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const snap = core.snapshot()
  await writeBlocks(core, 2, { start: 2 })

  {
    const rx = snap.read()
    const proms = [rx.getBlock(0), rx.getBlock(1), rx.getBlock(2)]
    rx.tryFlush()
    const res = await Promise.all(proms)
    t.is(b4a.toString(res[0]), 'block0')
    t.is(b4a.toString(res[1]), 'block1')
    t.is(res[2], null)
  }

  {
    const rx = core.read()
    const p = rx.getBlock(2)
    rx.tryFlush()
    t.is(b4a.toString(await p), 'block2', 'sanity check: does exist in non-snapshot core')
  }
})

test('delete hypercore block', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const tx = core.write()

  tx.deleteBlock(0)
  tx.deleteBlock(2) // doesn't exist
  await tx.flush()

  const rx = core.read()
  const p = Promise.all([rx.getBlock(0), rx.getBlock(1), rx.getBlock(2)])
  rx.tryFlush()
  const [res0, res1, res2] = await p
  t.is(res0, null)
  t.is(b4a.toString(res1), 'block1')
  t.is(res2, null)
})

test('delete hypercore block range', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 4)

  const tx = core.write()

  tx.deleteBlockRange(1, 3)
  await tx.flush()

  const rx = core.read()
  const p = Promise.all([
    rx.getBlock(0),
    rx.getBlock(1),
    rx.getBlock(2),
    rx.getBlock(3)
  ])
  rx.tryFlush()
  const [res0, res1, res2, res3] = await p
  t.is(b4a.toString(res0), 'block0')
  t.is(res1, null)
  t.is(res2, null)
  t.is(b4a.toString(res3), 'block3')
})

test('put and get tree node', async (t) => {
  const core = await createCore(t)

  const node1 = {
    index: 0,
    size: 1,
    hash: b4a.from('a'.repeat(64), 'hex')
  }
  const node2 = {
    index: 1,
    size: 10,
    hash: b4a.from('b'.repeat(64), 'hex')
  }

  const tx = core.write()
  tx.putTreeNode(node1)
  tx.putTreeNode(node2)
  await tx.flush()

  const rx = core.read()
  const p = Promise.all([rx.getTreeNode(0), rx.getTreeNode(1), rx.getTreeNode(2)])
  rx.tryFlush()
  const [res0, res1, res2] = await p

  t.alike(res0, node1)
  t.alike(res1, node2)
  t.is(res2, null)
})

test('delete tree node', async (t) => {
  const core = await createCore(t)

  const node0 = {
    index: 0,
    size: 1,
    hash: b4a.from('a'.repeat(64), 'hex')
  }
  const node1 = {
    index: 1,
    size: 10,
    hash: b4a.from('b'.repeat(64), 'hex')
  }

  {
    const tx = core.write()
    tx.putTreeNode(node0)
    tx.putTreeNode(node1)
    await tx.flush()
  }

  {
    const tx = core.write()
    tx.deleteTreeNode(0)
    tx.deleteTreeNode(10) // Doesn't exist
    await tx.flush()
  }

  const rx = core.read()
  const p = Promise.all([rx.getTreeNode(0), rx.getTreeNode(1), rx.getTreeNode(2)])
  rx.tryFlush()
  const [res0, res1] = await p

  t.is(res0, null)
  t.alike(res1, node1)
})

test('delete tree node range', async (t) => {
  const core = await createCore(t)

  const node0 = {
    index: 0,
    size: 1,
    hash: b4a.from('a'.repeat(64), 'hex')
  }
  const node1 = {
    index: 1,
    size: 10,
    hash: b4a.from('b'.repeat(64), 'hex')
  }
  const node2 = {
    index: 2,
    size: 20,
    hash: b4a.from('c'.repeat(64), 'hex')
  }
  const node3 = {
    index: 3,
    size: 30,
    hash: b4a.from('d'.repeat(64), 'hex')
  }

  {
    const tx = core.write()
    tx.putTreeNode(node0)
    tx.putTreeNode(node1)
    tx.putTreeNode(node2)
    tx.putTreeNode(node3)
    await tx.flush()
  }

  {
    const tx = core.write()
    tx.deleteTreeNodeRange(1, 3)
    await tx.flush()
  }

  const rx = core.read()
  const p = Promise.all([rx.getTreeNode(0), rx.getTreeNode(1), rx.getTreeNode(2), rx.getTreeNode(3)])
  rx.tryFlush()
  const [res0, res1, res2, res3] = await p

  t.alike(res0, node0)
  t.is(res1, null)
  t.is(res2, null)
  t.alike(res3, node3)
})

async function writeBlocks (core, amount, { start = 0, pre = '' } = {}) {
  const tx = core.write()
  for (let i = start; i < amount + start; i++) {
    tx.putBlock(i, `${pre}block${i}`)
  }
  await tx.flush()
}
