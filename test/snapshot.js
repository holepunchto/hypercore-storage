const test = require('brittle')
const b4a = require('b4a')
const {
  createCore,
  writeBlocks,
  readBlocks
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

test.skip('cannot write to snapshot', async (t) => {
  const core = await createCore(t)

  const snap = core.snapshot()
  await writeBlocks(snap, 2)

  // TODO: verify that writing to a snapshot shouldn't just throw
  t.alike(await readBlocks(snap, 3), [null, null, null], 'Cannot write to a snaphot (noop)')

  // TODO: clarify ([ <Buffer 62 6c 6f 63 6b 30>, <Buffer 62 6c 6f 63 6b 31>, null ])
  t.alike(await readBlocks(core, 3), [null, null, null], 'Writing to a snapshot has no impact on actual core')
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
