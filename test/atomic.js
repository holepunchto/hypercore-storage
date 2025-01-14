const test = require('brittle')
const b4a = require('b4a')
const { createCore, writeBlocks } = require('./helpers')

test('basic atomized flow with a single core', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const initBlocks = [b4a.from('block0'), b4a.from('block1')]
  t.alike(await readBlocks(core, 3), [...initBlocks, null], 'sanity check')

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  await writeBlocks(atomCore, 1, { start: 2 })
  const expected = [...initBlocks, b4a.from('block2'), null]

  t.alike(await readBlocks(core, 4), [...initBlocks, null, null], 'not added to original core')
  t.alike(await readBlocks(atomCore, 4), expected, 'added to atomized core')

  await atom.flush()

  t.alike(await readBlocks(core, 4), expected, 'flushing adds to the original core')
  t.alike(await readBlocks(atomCore, 4), expected, 'added to atomized core')
})

test('write to original core while there is an atomized one', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)
  const initBlocks = [b4a.from('block0'), b4a.from('block1')]

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  await writeBlocks(core, 1, { start: 2 })

  {
    const expected = [...initBlocks, b4a.from('block2'), null]
    t.alike(await readBlocks(core, 4), expected, 'added to original core')
    t.alike(await readBlocks(atomCore, 4), expected, 'added to atomized core')
  }
})

test('first writes to a core are from an atom', async (t) => {
  const core = await createCore(t)

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  await writeBlocks(atomCore, 1)

  const expected = [b4a.from('block0'), null]
  t.alike(await readBlocks(atomCore, 2), expected, 'added to atom core')
  t.alike(await readBlocks(core, 2), [null, null], 'not yet added to original')
  await atom.flush()
  t.alike(await readBlocks(atomCore, 2), expected, 'added to original after flush')
})

test('atomized flow with write/delete operations on a single core', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 3)

  const initBlocks = [0, 1, 2].map(i => b4a.from(`block${i}`))
  t.alike(await readBlocks(core, 4), [...initBlocks, null], 'sanity check')

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  {
    const tx = atomCore.write()
    tx.deleteBlock(1)
    tx.deleteBlock(4) // doesn't exist yet
    await tx.flush()
  }
  await writeBlocks(atomCore, 3, { start: 3 })

  const expected = [
    b4a.from('block0'),
    null,
    b4a.from('block2'),
    b4a.from('block3'),
    b4a.from('block4'),
    b4a.from('block5'),
    null
  ]
  t.alike(await readBlocks(atomCore, 7), expected)
  t.alike(await readBlocks(core, 7), [...initBlocks, null, null, null, null], 'original not yet updated')

  await atom.flush()
  t.alike(await readBlocks(core, 7), expected)
})

test('atomized flow with all non-delete operations on a single core', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  {
    await writeBlocks(atomCore, 2, { start: 2 })

    const tx = atomCore.write()
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

  const expBlocks = [b4a.from('block0'), b4a.from('block1'), b4a.from('block2'), b4a.from('block3'), null]
  const expNodes = [
    {
      index: 0,
      size: 1,
      hash: b4a.from('a'.repeat(64), 'hex')
    },
    null
  ]
  const expAuth = {
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32),
    manifest: null,
    keyPair: null,
    encryptionKey: b4a.from('a'.repeat(64, 'hex'))
  }
  const expHead = {
    fork: 1,
    length: 3,
    rootHash: b4a.from('a'.repeat(64), 'hex'),
    signature: b4a.from('b'.repeat(64), 'hex')
  }
  const expDependency = {
    dataPointer: 1,
    length: 3
  }
  const expHints = {
    contiguousLength: 1
  }
  const expBitfields = [b4a.from('bitfield-data-1'), null]

  t.alike(await readBlocks(atomCore, 5), expBlocks, 'blocks atom')
  t.alike(
    await readBlocks(core, 5),
    [b4a.from('block0'), b4a.from('block1'), null, null, null],
    'blocks orig pre flush'
  )

  t.alike(await readTreeNodes(atomCore, 2), expNodes, 'tree nodes atom')
  t.alike(await readTreeNodes(core, 2), [null, null], 'tree nodes orig pre flush')

  t.alike(await getAuth(atomCore), expAuth, 'auth atom')
  t.alike(
    await getAuth(core),
    {
      key: b4a.alloc(32),
      discoveryKey: b4a.alloc(32),
      manifest: null,
      keyPair: null,
      encryptionKey: null
    },
    'auth orig pre flush'
  )

  t.alike(await getHead(atomCore), expHead, 'head atom')
  t.alike(await getHead(core), null, 'head orig pre flush')

  t.alike(await getDependency(atomCore), expDependency, 'dependency atom')
  t.alike(await getDependency(core), null, 'dependency orig pre flush')

  t.alike(await getHints(atomCore), expHints, 'hints atom')
  t.alike(await getHints(core), null, 'hints orig pre flush')

  t.alike(await getUserData(atomCore, 'key'), b4a.from('value'), 'userdata atom')
  t.alike(await getUserData(core, 'key'), null, 'userdata orig pre flush')

  t.alike(await getBitfieldPages(atomCore, 2), expBitfields, 'bitfields atom')
  t.alike(await getBitfieldPages(core, 2), [null, null], 'bitfields orig pre flush')

  await atom.flush()
  t.alike(await readBlocks(core, 5), expBlocks, 'blocks orig post flush')
  t.alike(await readTreeNodes(core, 2), expNodes, 'tree nodes orig post flush')
  t.alike(await getAuth(core, 2), expAuth, 'auth orig post flush')
  t.alike(await getHead(core), expHead, 'head orig post flush')
  t.alike(await getDependency(core), expDependency, 'dependency orig post flush')
  t.alike(await getHints(core), expHints, 'hints orig post flush')
  t.alike(await getUserData(core, 'key'), b4a.from('value'), 'userdata orig post flush')
  t.alike(await getBitfieldPages(core, 2), expBitfields, 'bitfields orig post flush')
})

async function readBlocks (core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getBlock(i))
  rx.tryFlush()
  return await Promise.all(proms)
}

async function readTreeNodes (core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getTreeNode(i))
  rx.tryFlush()
  return await Promise.all(proms)
}

async function getAuth (core) {
  const rx = core.read()
  const p = rx.getAuth()
  rx.tryFlush()
  return await p
}

async function getHead (core) {
  const rx = core.read()
  const p = rx.getHead()
  rx.tryFlush()
  return await p
}

async function getDependency (core) {
  const rx = core.read()
  const p = rx.getDependency()
  rx.tryFlush()
  return await p
}

async function getHints (core) {
  const rx = core.read()
  const p = rx.getHints()
  rx.tryFlush()
  return await p
}

async function getUserData (core, key) {
  const rx = core.read()
  const p = rx.getUserData(key)
  rx.tryFlush()
  return await p
}

async function getBitfieldPages (core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getBitfieldPage(i))
  rx.tryFlush()
  return await Promise.all(proms)
}
