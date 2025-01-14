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

async function readBlocks (core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getBlock(i))
  rx.tryFlush()
  return await Promise.all(proms) // ).map(b => b4a.toString(b))
}
