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
  {
    const expected = [...initBlocks, b4a.from('block2'), null]
    t.alike(await readBlocks(core, 4), [...initBlocks, null, null], 'not added to original core')
    t.alike(await readBlocks(atomCore, 4), expected, 'added to atomized core')
  }
})

test('write to original core while there is an atomized one', async (t) => {
  const core = await createCore(t)
  await writeBlocks(core, 2)
  const initBlocks = [b4a.from('block0'), b4a.from('block1')]

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  console.log('wrting more')
  await writeBlocks(core, 1, { start: 2 })

  {
    const expected = [...initBlocks, b4a.from('block2'), null]
    t.alike(await readBlocks(core, 4), expected, 'added to original core')
    t.alike(await readBlocks(atomCore, 4), expected, 'added to atomized core')
  }
})

async function readBlocks (core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getBlock(i))
  rx.tryFlush()
  return await Promise.all(proms) // ).map(b => b4a.toString(b))
}
