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

test('set and get auth', async (t) => {
  const core = await createCore(t)

  {
    const rx = core.read()
    const p = rx.getAuth()
    rx.tryFlush()
    const initAuth = await p
    t.alike(
      initAuth,
      {
        key: b4a.alloc(32),
        discoveryKey: b4a.alloc(32),
        manifest: null,
        keyPair: null,
        encryptionKey: null
      },
      'fresh core auth'
    )
  }

  {
    const tx = core.write()
    tx.setAuth({
      key: b4a.alloc(32),
      discoveryKey: b4a.alloc(32),
      manifest: null,
      keyPair: null,
      encryptionKey: b4a.from('a'.repeat(64, 'hex'))
    })
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = rx.getAuth()
    rx.tryFlush()
    t.alike(
      await p,
      {
        key: b4a.alloc(32),
        discoveryKey: b4a.alloc(32),
        manifest: null,
        keyPair: null,
        encryptionKey: b4a.from('a'.repeat(64, 'hex'))
      },
      'updated auth'
    )
  }
})

test('set and get hypercore blocks', async (t) => {
  const core = await createCore(t)
  {
    const rx = core.read()
    const p = rx.getSessions()
    rx.tryFlush()
    t.alike(await p, null, 'No sessions on init core')
  }

  {
    const tx = core.write()
    tx.setSessions([
      { name: 'session0', dataPointer: 0 },
      { name: 'session1', dataPointer: 1 }
    ])
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = rx.getSessions()
    rx.tryFlush()
    t.alike(
      await p,
      [
        { name: 'session0', dataPointer: 0 },
        { name: 'session1', dataPointer: 1 }
      ]
    )
  }
})

test('set and get hypercore head', async (t) => {
  const core = await createCore(t)
  {
    const rx = core.read()
    const p = rx.getHead()
    rx.tryFlush()
    t.alike(await p, null, 'No head on init core')
  }

  {
    const tx = core.write()
    tx.setHead({
      fork: 1,
      length: 3,
      rootHash: b4a.from('a'.repeat(64), 'hex'),
      signature: b4a.from('b'.repeat(64), 'hex')
    })
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = rx.getHead()
    rx.tryFlush()
    t.alike(
      await p,
      {
        fork: 1,
        length: 3,
        rootHash: b4a.from('a'.repeat(64), 'hex'),
        signature: b4a.from('b'.repeat(64), 'hex')
      },
      'updated head')
  }
})

test('set and get hypercore dependency', async (t) => {
  const core = await createCore(t)
  {
    const rx = core.read()
    const p = rx.getDependency()
    rx.tryFlush()
    t.alike(await p, null, 'No dependency on init core')
  }

  {
    const tx = core.write()
    tx.setDependency({
      dataPointer: 1,
      length: 3
    })
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = rx.getDependency()
    rx.tryFlush()
    t.alike(
      await p,
      {
        dataPointer: 1,
        length: 3
      },
      'updated dependency')
  }
})

test('set and get hypercore hints', async (t) => {
  const core = await createCore(t)
  {
    const rx = core.read()
    const p = rx.getHints()
    rx.tryFlush()
    console.log(await p)
    t.alike(await p, null, 'No hints on init core')
  }

  {
    const tx = core.write()
    tx.setHints({
      contiguousLength: 1
    })
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = rx.getHints()
    rx.tryFlush()
    console.log(await p)
    t.alike(
      await p,
      { contiguousLength: 1 },
      'updated hints')
  }
})

test('set and get hypercore userdata', async (t) => {
  const core = await createCore(t)
  {
    const rx = core.read()
    const p = rx.getUserData()
    rx.tryFlush()
    t.alike(await p, null, 'No userdata on init core')
  }

  {
    const tx = core.write()
    tx.putUserData('key', 'value')
    tx.putUserData('key2', 'value2')
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getUserData('key'),
      rx.getUserData('key2'),
      rx.getUserData('no-key')
    ])
    rx.tryFlush()
    const [data1, data2, data3] = await p

    t.is(b4a.toString(data1), 'value')
    t.is(b4a.toString(data2), 'value2')
    t.is(data3, null)
  }
})

test('delete hypercore userdata', async (t) => {
  const core = await createCore(t)

  {
    const tx = core.write()
    tx.putUserData('key', 'value')
    tx.putUserData('key2', 'value2')
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getUserData('key'),
      rx.getUserData('key2')
    ])
    rx.tryFlush()
    const [data1, data2] = await p

    t.is(b4a.toString(data1), 'value', 'sanity check')
    t.is(b4a.toString(data2), 'value2', 'sanity check')
  }

  {
    const tx = core.write()
    tx.deleteUserData('key')
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getUserData('key'),
      rx.getUserData('key2')
    ])
    rx.tryFlush()
    const [data1, data2] = await p

    t.is(data1, null, 'deleted')
    t.is(b4a.toString(data2), 'value2')
  }
})

test('set and get bitfield page', async (t) => {
  const core = await createCore(t)

  {
    // Note: not sure these values are valid bitfield data
    // but the API seems to accept generic buffers
    const tx = core.write()
    tx.putBitfieldPage(0, 'bitfield-data-1')
    tx.putBitfieldPage(1, 'bitfield-data-2')
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getBitfieldPage(0),
      rx.getBitfieldPage(1),
      rx.getBitfieldPage(2)
    ])
    rx.tryFlush()
    const [data1, data2, data3] = await p

    t.is(b4a.toString(data1), 'bitfield-data-1')
    t.is(b4a.toString(data2), 'bitfield-data-2')
    t.is(data3, null)
  }
})

test('delete bitfield page', async (t) => {
  const core = await createCore(t)

  {
    const tx = core.write()
    tx.putBitfieldPage(0, 'bitfield-data-1')
    tx.putBitfieldPage(1, 'bitfield-data-2')
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getBitfieldPage(0),
      rx.getBitfieldPage(1)
    ])
    rx.tryFlush()
    const [data1, data2] = await p

    t.is(b4a.toString(data1), 'bitfield-data-1', 'sanity check')
    t.is(b4a.toString(data2), 'bitfield-data-2', 'sanity check')
  }

  {
    const tx = core.write()
    tx.deleteBitfieldPage(0)
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getBitfieldPage(0),
      rx.getBitfieldPage(1)
    ])
    rx.tryFlush()
    const [data1, data2] = await p

    t.is(data1, null, 'deleted')
    t.is(b4a.toString(data2), 'bitfield-data-2', 'sanity check')
  }
})

test('delete bitfield page range', async (t) => {
  const core = await createCore(t)

  {
    const tx = core.write()
    tx.putBitfieldPage(0, 'bitfield-data-1')
    tx.putBitfieldPage(1, 'bitfield-data-2')
    tx.putBitfieldPage(2, 'bitfield-data-3')
    tx.putBitfieldPage(3, 'bitfield-data-4')
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getBitfieldPage(0),
      rx.getBitfieldPage(1),
      rx.getBitfieldPage(2),
      rx.getBitfieldPage(3)
    ])
    rx.tryFlush()
    const [data1, data2, data3, data4] = await p

    t.is(b4a.toString(data1), 'bitfield-data-1', 'sanity check')
    t.is(b4a.toString(data2), 'bitfield-data-2', 'sanity check')
    t.is(b4a.toString(data3), 'bitfield-data-3', 'sanity check')
    t.is(b4a.toString(data4), 'bitfield-data-4', 'sanity check')
  }

  {
    const tx = core.write()
    tx.deleteBitfieldPageRange(1, 3)
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = Promise.all([
      rx.getBitfieldPage(0),
      rx.getBitfieldPage(1),
      rx.getBitfieldPage(2),
      rx.getBitfieldPage(3)
    ])
    rx.tryFlush()
    const [data1, data2, data3, data4] = await p

    t.is(b4a.toString(data1), 'bitfield-data-1')
    t.is(data2, null)
    t.is(data3, null)
    t.is(b4a.toString(data4), 'bitfield-data-4')
  }
})

async function writeBlocks (core, amount, { start = 0, pre = '' } = {}) {
  const tx = core.write()
  for (let i = start; i < amount + start; i++) {
    tx.putBlock(i, `${pre}block${i}`)
  }
  await tx.flush()
}
