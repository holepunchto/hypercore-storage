const test = require('brittle')
const b4a = require('b4a')
const { createCore, toArray } = require('./helpers')

test('block stream', async function (t) {
  const core = await createCore(t)

  const tx = core.write()
  const expected = []

  for (let i = 0; i < 10; i++) {
    tx.putBlock(i, b4a.from([i]))
    expected.push({ index: i, value: b4a.from([i]) })
  }

  await tx.flush()

  const blocks = await toArray(core.createBlockStream(0, 10, false))

  t.alike(blocks, expected)
})

test('reverse block stream', async function (t) {
  const core = await createCore(t)

  const tx = core.write()
  const expected = []

  for (let i = 0; i < 10; i++) {
    tx.putBlock(i, b4a.from([i]))
    expected.push({ index: i, value: b4a.from([i]) })
  }

  await tx.flush()

  const blocks = await toArray(core.createBlockStream(0, 10, true))

  t.alike(blocks, expected.reverse())
})

test('block stream (atom)', async function (t) {
  const core = await createCore(t)
  const atom = core.atom()

  const a = core.atomize(atom)

  const expected = []

  {
    const tx = a.write()

    for (let i = 0; i < 5; i++) {
      const index = 2 * i
      tx.putBlock(index, b4a.from([index]))
      expected.push({ index, value: b4a.from([index]) })
    }

    await tx.flush()
  }

  await atom.flush()

  {
    const tx = a.write()

    for (let i = 0; i < 5; i++) {
      const index = 2 * i + 1
      tx.putBlock(index, b4a.from([index]))
      expected.push({ index, value: b4a.from([index]) })
    }

    await tx.flush()
  }

  {
    const blocks = await toArray(a.createBlockStream(0, 10, false))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream(0, 10, true))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  {
    const tx = a.write()
    tx.deleteBlockRange(4, 6)
    await tx.flush()
  }

  expected.sort(cmpBlock).splice(4, 2)

  {
    const blocks = await toArray(a.createBlockStream(0, 10, false))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream(0, 10, true))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  {
    const tx = a.write()
    tx.deleteBlockRange(0, 2)
    tx.deleteBlockRange(8, 9)
    await tx.flush()
  }

  expected.sort(cmpBlock)

  expected.shift()
  expected.shift()
  const tmp = expected.pop()
  expected.pop()
  expected.push(tmp)

  {
    const blocks = await toArray(a.createBlockStream(0, 10, false))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream(0, 10, true))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  {
    const tx = a.write()
    tx.deleteBlockRange(8, 10)
    await tx.flush()
  }

  expected.sort(cmpBlock)
  expected.pop()

  {
    const blocks = await toArray(a.createBlockStream(0, 10, false))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream(0, 10, true))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }

  t.ok(a.view.changes, 'used atomic view')

  await atom.flush()

  {
    const blocks = await toArray(a.createBlockStream(0, 10, false))
    t.alike(blocks, expected.sort(cmpBlock))
  }

  {
    const blocks = await toArray(a.createBlockStream(0, 10, true))
    t.alike(blocks, expected.sort(cmpBlock).reverse())
  }
})

function cmpBlock (a, b) {
  return a.index - b.index
}
