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
