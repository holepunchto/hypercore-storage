const test = require('brittle')
const tmp = require('test-tmp')
const CoreStorage = require('../')

const DK_0 = Buffer.alloc(32).fill('dk0')
const HASH = Buffer.alloc(32).fill('hash')

test('basic', async function (t) {
  const dir = await tmp(t)

  const s = new CoreStorage(dir)

  const c = s.get(DK_0)

  t.is(await c.open(), false)
  await c.create()

  {
    const b = c.createWriteBatch()

    b.addTreeNode({
      index: 42,
      hash: HASH,
      size: 10
    })

    b.addTreeNode({
      index: 43,
      hash: HASH,
      size: 2
    })

    await b.flush()
  }

  {
    const b = c.createReadBatch()
    const node1 = b.getTreeNode(42)
    const node2 = b.getTreeNode(43)
    b.tryFlush()

    t.alike(await node1, { index: 42, hash: HASH, size: 10 })
    t.alike(await node2, { index: 43, hash: HASH, size: 2 })
  }

  await s.close()
})
