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

test.solo('cannot write to snapshot', async (t) => {
  const core = await createCore(t)

  const snap = core.snapshot()
  await writeBlocks(snap, 2)

  // TODO: verify that writing to a snapshot shouldn't just throw
  t.alike(await readBlocks(snap, 3), [null, null, null], 'Cannot write to a snaphot (noop)')

  // TODO: clarify ([ <Buffer 62 6c 6f 63 6b 30>, <Buffer 62 6c 6f 63 6b 31>, null ])
  t.alike(await readBlocks(core, 3), [null, null, null], 'Writing to a snapshot has no impact on actual core')
})
