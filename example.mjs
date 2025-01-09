import RocksDB from 'rocksdb-native'
import Storage from './index.js'

const s = new Storage(new RocksDB('/tmp/my-corestore'))

// reset it first for simplicity
await s.clear()

for await (const data of s.createCoreStream()) {
  console.log(data, '<--')
}

const core1 = await s.create({ key: Buffer.alloc(32, 0), discoveryKey: Buffer.alloc(32, 0) })
const core2 = await s.create({ key: Buffer.alloc(32, 1), discoveryKey: Buffer.alloc(32, 1) })

const atom = s.atom()

const a = core1.atomize(atom)
const b = core2.atomize(atom)

{
  const w = a.write()
  w.putBlock(0, Buffer.from('core1 block'))
  await w.flush()
}

{
  const w = b.write()
  w.putBlock(0, Buffer.from('core2 block'))
  await w.flush()
}

console.log(a.view === b.view, a.view)

{
  const r = a.read()
  const p = r.getBlock(0)
  r.tryFlush()
  console.log('core1 block:', await p)
}

{
  const r = b.read()
  const p = r.getBlock(0)
  r.tryFlush()
  console.log('core2 block:', await p)
}

await atom.flush()

const snap = a.snapshot()

{
  const w = a.write()
  w.putBlock(0, Buffer.from('OVERWRITE'))
  await w.flush()
}

{
  const r = snap.read()
  const p = r.getBlock(0)
  r.tryFlush()
  console.log('core1 (snap) block:', (await p).toString())
}

{
  const r = a.read()
  const p = r.getBlock(0)
  r.tryFlush()
  console.log('core1 block:', (await p).toString())
}

await atom.flush()

{
  const r = snap.read()
  const p = r.getBlock(0)
  r.tryFlush()
  console.log('core1 (snap) block:', (await p).toString())
}

{
  const r = a.read()
  const p = r.getBlock(0)
  r.tryFlush()
  console.log('core1 block:', (await p).toString())
}

// // const w = core.write()

// // w.putBlock(0, Buffer.from('hello'))
// // w.putBlock(1, Buffer.from('world'))

// // await w.flush()

// const snap = core.snapshot()

// for await (const data of snap.createBlockStream()) {
//   console.log(data)
// }

// await snap.close()
