const CoreStorage = require('../')
const tmp = require('test-tmp')
const test = require('brittle')

const DK_0 = Buffer.alloc(32).fill('dk0')
const DK_1 = Buffer.alloc(32).fill('dk1')

test('basic cross atomic batches', async function (t) {
  const s = new CoreStorage(await tmp(t))

  const atom = s.atom()
  const a = await s.create({ key: DK_0, discoveryKey: DK_0 })
  const b = await s.create({ key: DK_1, discoveryKey: DK_1 })

  let waited = false

  const w1 = a.createWriteBatch(atom)
  const w2 = b.createWriteBatch(atom)

  w1.putBlock(0, Buffer.from('block #0'))
  const promise = w1.flush()
  promise.then(() => t.ok(waited))

  w2.putBlock(0, Buffer.from('another block #0'))
  await new Promise(resolve => setTimeout(resolve, 1000))
  waited = true
  await w2.flush()
  await promise

  await s.close()
})
