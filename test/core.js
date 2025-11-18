const test = require('brittle')
const b4a = require('b4a')
const View = require('../lib/view.js')
const { CoreRX, CorestoreRX } = require('../lib/tx.js')
const { createCore, create, writeBlocks, readBlocks } = require('./helpers')

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
    storage.createCore(keys0),
    storage.createCore(keys1),
    storage.createCore(keys2)
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
  const p = Promise.all([rx.getBlock(0), rx.getBlock(1), rx.getBlock(2), rx.getBlock(3)])
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
  const p = Promise.all([
    rx.getTreeNode(0),
    rx.getTreeNode(1),
    rx.getTreeNode(2),
    rx.getTreeNode(3)
  ])
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

test('CoreRX - static getAuth', async (t) => {
  const s = await create(t)
  const discoveryKey = b4a.alloc(32)
  const core = await s.createCore({
    key: b4a.alloc(32),
    discoveryKey
  })

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
    const EMPTY = new View()
    const rx = new CorestoreRX(s.db, EMPTY)
    const coreProm = rx.getCore(discoveryKey)
    rx.tryFlush()
    const c = await coreProm

    const read = s.db.read({ autoDestroy: true })
    const authPromise = CoreRX.getAuth(s.db, c)
    read.tryFlush()
    t.alike(
      await authPromise,
      {
        key: b4a.alloc(32),
        discoveryKey: b4a.alloc(32),
        manifest: null,
        keyPair: null,
        encryptionKey: b4a.from('a'.repeat(64, 'hex'))
      },
      'got auth'
    )
  }

  await core.close()
  await s.close()
})

test('set and get hypercore sessions', async (t) => {
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
    t.alike(await p, [
      { name: 'session0', dataPointer: 0 },
      { name: 'session1', dataPointer: 1 }
    ])
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
      'updated head'
    )
  }
})

test('CoreRX - static getHead', async (t) => {
  const s = await create(t)
  const discoveryKey = b4a.alloc(32)
  const core = await s.createCore({
    key: b4a.alloc(32),
    discoveryKey
  })

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
    const EMPTY = new View()
    const rx = new CorestoreRX(s.db, EMPTY)
    const coreProm = rx.getCore(discoveryKey)
    rx.tryFlush()
    const c = await coreProm

    const read = s.db.read({ autoDestroy: true })
    const headProm = CoreRX.getHead(s.db, c)
    read.tryFlush()
    t.alike(
      await headProm,
      {
        fork: 1,
        length: 3,
        rootHash: b4a.from('a'.repeat(64), 'hex'),
        signature: b4a.from('b'.repeat(64), 'hex')
      },
      'got head'
    )
  }

  await core.close()
  await s.close()
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
      'updated dependency'
    )
  }
})

test('set and get hypercore hints', async (t) => {
  const core = await createCore(t)
  {
    const rx = core.read()
    const p = rx.getHints()
    rx.tryFlush()
    t.alike(await p, null, 'No hints on init core')
  }

  {
    const tx = core.write()
    tx.setHints({
      contiguousLength: 2,
      remoteContiguousLength: 1
    })
    await tx.flush()
  }

  {
    const rx = core.read()
    const p = rx.getHints()
    rx.tryFlush()
    t.alike(await p, { contiguousLength: 2, remoteContiguousLength: 1 }, 'updated hints')
  }
})

test('CoreRX - static getHints', async (t) => {
  const s = await create(t)
  const discoveryKey = b4a.alloc(32)
  const core = await s.createCore({
    key: b4a.alloc(32),
    discoveryKey
  })

  {
    const rx = core.read()
    const p = rx.getHints()
    rx.tryFlush()
    t.alike(await p, null, 'No hints on init core')
  }

  {
    const tx = core.write()
    tx.setHints({
      contiguousLength: 1337,
      remoteContiguousLength: 123
    })
    await tx.flush()
  }

  {
    const EMPTY = new View()
    const rx = new CorestoreRX(s.db, EMPTY)
    const coreProm = rx.getCore(discoveryKey)
    rx.tryFlush()
    const c = await coreProm

    const read = s.db.read({ autoDestroy: true })
    const hintsPromise = CoreRX.getHints(s.db, c)
    read.tryFlush()
    t.alike(await hintsPromise, { contiguousLength: 1337, remoteContiguousLength: 123 }, 'got hint')
  }

  await core.close()
  await s.close()
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
    const p = Promise.all([rx.getUserData('key'), rx.getUserData('key2'), rx.getUserData('no-key')])
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
    const p = Promise.all([rx.getUserData('key'), rx.getUserData('key2')])
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
    const p = Promise.all([rx.getUserData('key'), rx.getUserData('key2')])
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
    const p = Promise.all([rx.getBitfieldPage(0), rx.getBitfieldPage(1), rx.getBitfieldPage(2)])
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
    const p = Promise.all([rx.getBitfieldPage(0), rx.getBitfieldPage(1)])
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
    const p = Promise.all([rx.getBitfieldPage(0), rx.getBitfieldPage(1)])
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

test('cannot open tx on snapshot', async (t) => {
  const core = await createCore(t)

  const snap = core.snapshot()
  t.exception(() => snap.write(), /Cannot open core tx on snapshot/)
})

test('cannot create sessions on snapshot', async (t) => {
  const core = await createCore(t)
  const snap = core.snapshot()

  await t.exception(async () => await snap.createSession(), /Cannot open core tx on snapshot/)
})

test('can resume a snapshot session, and that session is a snapshot too', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const snap = core.snapshot()
  const session = await core.createSession('sess', null)
  const sessionSnap = session.snapshot()

  t.is(session.snapshotted, false, 'sanity check')

  const initBlocks = [b4a.from('block0'), b4a.from('block1'), null]
  t.alike(await readBlocks(snap, 3), initBlocks, 'sanity check snap')
  t.alike(await readBlocks(session, 3), [null, null, null], 'sanity check session')
  t.alike(await readBlocks(sessionSnap, 3), [null, null, null], 'sanity check snap session')

  await writeBlocks(core, 1, { pre: 'core-', start: 2 })
  await writeBlocks(session, 1, { pre: 'sess-', start: 2 })
  t.alike(
    await readBlocks(session, 3),
    [null, null, b4a.from('sess-block2')],
    'session updated (sanity check)'
  )
  t.alike(
    await readBlocks(core, 3),
    [b4a.from('block0'), b4a.from('block1'), b4a.from('core-block2')],
    'core updated (sanity check)'
  )
  t.alike(await readBlocks(snap, 3), initBlocks, 'snap did not change (sanity check)')
  t.alike(
    await readBlocks(sessionSnap, 3),
    [null, null, null],
    'post-session snap did not change (sanity check)'
  )

  const resumedSnapSession = await sessionSnap.resumeSession('sess')
  const resumedSession = await core.resumeSession('sess')

  t.is(resumedSnapSession.snapshotted, true, 'resumed snapshot session is snapshot')
  t.is(resumedSession.snapshotted, false, 'resumed session is not snapshot')
  t.alike(
    await readBlocks(resumedSession, 3),
    [null, null, b4a.from('sess-block2')],
    'resumed session changed like original session'
  )
  t.alike(
    await readBlocks(resumedSnapSession, 3),
    [null, null, null],
    'resumed snap session did not change'
  )
})

test('create named sessions', async (t) => {
  const core = await createCore(t)

  const tx = core.write()

  tx.setHead({
    length: 10,
    fork: 0,
    rootHash: b4a.alloc(32),
    signature: null
  })

  await tx.flush()

  const a = await core.createSession('a', null)
  const b = await core.resumeSession('a')
  const c = await b.resumeSession('a')

  t.is(a.core.dependencies.length, 1)
  t.is(b.core.dependencies.length, 1)
  t.is(c.core.dependencies.length, 1)

  t.is(a.core.dependencies[0].length, 10)
  t.is(b.core.dependencies[0].length, 10)
  t.is(c.core.dependencies[0].length, 10)
})

test('export hypercore', async (t) => {
  const s = await create(t)
  const core = await s.createCore({
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32)
  })

  // Note: not sure these values are valid bitfield data
  // but the API seems to accept generic buffers
  const head = {
    fork: 1,
    length: 3,
    rootHash: b4a.from('a'.repeat(64), 'hex'),
    signature: b4a.from('b'.repeat(64), 'hex')
  }

  const node0 = { index: 0, size: 1, hash: b4a.from('a'.repeat(64), 'hex') }
  const node1 = { index: 1, size: 10, hash: b4a.from('b'.repeat(64), 'hex') }

  const tx = core.write()

  tx.setHead(head)
  tx.setAuth({
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32),
    manifest: null,
    keyPair: {
      secretKey: b4a.alloc(64, 1),
      publicKey: b4a.alloc(32, 2)
    },
    encryptionKey: b4a.from('a'.repeat(64), 'hex')
  })

  tx.putBitfieldPage(0, 'bitfield-data-0')
  tx.putBitfieldPage(1, 'bitfield-data-1')

  tx.putBlock(0, 'content0')
  tx.putBlock(1, 'content1')

  tx.putTreeNode(node0)
  tx.putTreeNode(node1)

  await tx.flush()

  const exported = await s.export(b4a.alloc(32))

  t.alike(exported.head, head)
  t.alike(exported.auth, {
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32),
    manifest: null,
    keyPair: null,
    encryptionKey: b4a.from('a'.repeat(64), 'hex')
  })

  t.alike(exported.sessions, [])
  t.is(exported.data.length, 1)

  const session = exported.data[0]

  t.alike(session.blocks, [
    { index: 0, value: b4a.from('content0') },
    { index: 1, value: b4a.from('content1') }
  ])

  t.alike(session.tree, [node0, node1])

  t.alike(session.bitfield, [
    { index: 0, page: b4a.from('bitfield-data-0') },
    { index: 1, page: b4a.from('bitfield-data-1') }
  ])

  await core.close()
  await s.close()
})

test('export named sessions', async (t) => {
  const s = await create(t)
  const core = await s.createCore({
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32)
  })

  const head = {
    length: 10,
    fork: 0,
    rootHash: b4a.alloc(32),
    signature: null
  }

  const node0 = { index: 0, size: 1, hash: b4a.from('a'.repeat(64), 'hex') }
  const node1 = { index: 1, size: 10, hash: b4a.from('b'.repeat(64), 'hex') }
  const node2 = { index: 2, size: 1, hash: b4a.from('c'.repeat(64), 'hex') }

  {
    const tx = core.write()

    tx.setHead(head)

    tx.putBitfieldPage(0, 'bitfield-data-0')
    tx.putBitfieldPage(1, 'bitfield-data-1')

    tx.putBlock(0, 'content0')
    tx.putBlock(1, 'content1')

    tx.putTreeNode(node0)
    tx.putTreeNode(node1)

    await tx.flush()
  }

  const session = await core.createSession('a', null)

  {
    const tx = session.write()

    tx.putBlock(2, 'content2')
    tx.putTreeNode(node2)
    tx.putBitfieldPage(1, 'bitfield-data-2')

    await tx.flush()
  }

  const exported = await s.export(b4a.alloc(32), { batches: true })

  t.alike(exported.sessions, ['a'])

  const batch = exported.data[1]

  t.alike(batch.blocks, [{ index: 2, value: b4a.from('content2') }])
  t.alike(batch.tree, [node2])
  t.alike(batch.bitfield, [{ index: 1, page: b4a.from('bitfield-data-2') }])

  await core.close()
  await session.close()
  await s.close()
})

test('compact core', async (t) => {
  const s = await create(t)
  const core = await s.createCore({
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32)
  })

  const head = {
    length: 10,
    fork: 0,
    rootHash: b4a.alloc(32),
    signature: null
  }

  const node0 = { index: 0, size: 1, hash: b4a.from('a'.repeat(64), 'hex') }
  const node1 = { index: 1, size: 10, hash: b4a.from('b'.repeat(64), 'hex') }
  const node2 = { index: 2, size: 1, hash: b4a.from('c'.repeat(64), 'hex') }

  {
    const tx = core.write()

    tx.setHead(head)

    tx.putBitfieldPage(0, 'bitfield-data-0')
    tx.putBitfieldPage(1, 'bitfield-data-1')

    tx.putBlock(0, 'content0')
    tx.putBlock(1, 'content1')

    tx.putTreeNode(node0)
    tx.putTreeNode(node1)

    await tx.flush()
  }

  const session = await core.createSession('a', null)

  {
    const tx = session.write()

    tx.putBlock(2, 'content2')
    tx.putTreeNode(node2)
    tx.putBitfieldPage(1, 'bitfield-data-2')

    await tx.flush()
  }

  await t.execution(session.compact())
  await t.execution(core.compact())

  await core.close()
  await session.close()
  await s.close()
})
