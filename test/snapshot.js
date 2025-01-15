const test = require('brittle')
const b4a = require('b4a')
const {
  createCore,
  writeBlocks,
  readBlocks,
  readTreeNodes,
  getAuth,
  getHead,
  getDependency,
  getHints,
  getUserData,
  getBitfieldPages
} = require('./helpers')

test('read and write hypercore blocks from snapshot', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const snap = core.snapshot()
  await writeBlocks(core, 2, { start: 2 })

  {
    const res = await readBlocks(snap, 3)
    t.is(b4a.toString(res[0]), 'block0')
    t.is(b4a.toString(res[1]), 'block1')
    t.is(res[2], null)
  }

  {
    const res = await readBlocks(core, 3)
    t.is(b4a.toString(res[2]), 'block2', 'sanity check: does exist in non-snapshot core')
  }
})

test('snapshots from atomized core do not get updated', async (t) => {
  const core = await createCore(t)
  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  const atomInitSnap = atomCore.snapshot()
  const initSnap = core.snapshot()
  t.alike(await readBlocks(initSnap, 2), [null, null], 'sanity check')

  await writeBlocks(atomCore, 2)
  const expected = [b4a.from('block0'), b4a.from('block1')]

  t.alike(await readBlocks(atomInitSnap, 2), [null, null], 'init atom snap did not change')
  t.alike(await readBlocks(initSnap, 2), [null, null], 'init snap did not change')

  const atomPostWriteSnap = atomCore.snapshot()
  const corePostWriteSnap = core.snapshot()

  t.alike(await readBlocks(atomPostWriteSnap, 2), expected, 'sanity check')
  t.alike(await readBlocks(corePostWriteSnap, 2), [null, null], 'sanity check')
  t.alike(await readBlocks(atomInitSnap, 2), [null, null], 'init atom snap did not change')
  t.alike(await readBlocks(initSnap, 2), [null, null], 'init  snap did not change')

  await writeBlocks(atomCore, 2, { pre: 'override-' })
  const expectedOverride = [b4a.from('override-block0'), b4a.from('override-block1')]
  t.alike(await readBlocks(atomCore, 2), expectedOverride, 'sanity check')

  t.alike(await readBlocks(atomPostWriteSnap, 2), expected, 'post-write atom snap did not change')
  t.alike(await readBlocks(atomInitSnap, 2), [null, null], 'init atom snap did not change')

  await atom.flush()

  t.alike(await readBlocks(atomPostWriteSnap, 2), expected, 'prev atom snap did not change')
  t.alike(await readBlocks(atomInitSnap, 2), [null, null], 'init atom snap did not change')
  t.alike(await readBlocks(corePostWriteSnap, 2), [null, null], 'core post-write snap did not change')
  t.alike(await readBlocks(initSnap, 2), [null, null], 'init snap did not change')

  t.alike(await readBlocks(core, 2), expectedOverride, 'sanity check')
})

test('snapshots immutable (all operations)', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const snap = core.snapshot()

  {
    await writeBlocks(core, 2, { start: 2 })

    const tx = core.write()
    tx.putTreeNode({
      index: 0,
      size: 1,
      hash: b4a.from('a'.repeat(64), 'hex')
    })
    tx.setAuth({
      key: b4a.alloc(32),
      discoveryKey: b4a.alloc(32),
      manifest: null,
      keyPair: null,
      encryptionKey: b4a.from('a'.repeat(64, 'hex'))
    })
    tx.setHead({
      fork: 1,
      length: 3,
      rootHash: b4a.from('a'.repeat(64), 'hex'),
      signature: b4a.from('b'.repeat(64), 'hex')
    })
    tx.setDependency({
      dataPointer: 1,
      length: 3
    })
    tx.setHints({
      contiguousLength: 1
    })
    tx.putUserData('key', b4a.from('value'))
    tx.putBitfieldPage(0, b4a.from('bitfield-data-1'))

    await tx.flush()
  }

  t.alike(
    await readBlocks(core, 4),
    [b4a.from('block0'), b4a.from('block1'), b4a.from('block2'), b4a.from('block3')],
    'sanity check'
  )

  t.not(await readBlocks(core, 3), [null, null, null], 'sanity check (core itself got updated')
  t.alike(
    await readBlocks(snap, 4),
    [b4a.from('block0'), b4a.from('block1'), null, null],
    'snap blocks unchanged'
  )

  t.not(await readTreeNodes(core, 2), [null, null], 'sanity check (core itself got updated)')
  t.alike(await readTreeNodes(snap, 2), [null, null], 'tree nodes unchanged')

  const origAuth = {
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32),
    manifest: null,
    keyPair: null,
    encryptionKey: null
  }
  t.not(await getAuth(core), origAuth, 'sanity check (core itself got updated)')
  t.alike(await getAuth(snap), origAuth, 'auth unchanged')

  t.not(await getHead(core), null, 'sanity check (core itself got updated)')
  t.alike(await getHead(snap), null, 'head unchanged')

  t.not(await getDependency(core), null, 'sanity check (core itself got updated)')
  t.alike(await getDependency(snap), null, 'dependency unchanged')

  t.not(await getHints(core), null, 'sanity check (core itself got updated)')
  t.alike(await getHints(snap), null, 'hints unchanged')

  t.not(await getUserData(core, 'key'), null, 'sanity check (core itself got updated)')
  t.alike(await getUserData(snap, 'key'), null, 'userdata unchanged')

  t.not(await getBitfieldPages(core, 2), [null, null], 'sanity check (core itself got updated)')
  t.alike(await getBitfieldPages(snap, 2), [null, null], 'bitfield pages unchanged')
})
