const test = require('brittle')
const b4a = require('b4a')
const { createCore } = require('./helpers')

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

async function writeBlocks (core, amount, { start = 0 } = {}) {
  const tx = core.write()
  for (let i = start; i < amount + start; i++) {
    tx.putBlock(i, `block${i}`)
  }
  await tx.flush()
}
