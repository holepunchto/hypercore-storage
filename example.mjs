import RocksDB from 'rocksdb-native'
import Storage from './index.js'

const s = new Storage(new RocksDB('/tmp/my-corestore'))

// reset it first for simplicity
await s.clear()

for await (const data of s.createCoreStream()) {
  console.log(data, '<--')
}

const core = await s.create({ key: Buffer.alloc(32), discoveryKey: Buffer.alloc(32) })

// const w = core.write()

// w.putBlock(0, Buffer.from('hello'))
// w.putBlock(1, Buffer.from('world'))

// await w.flush()

const snap = core.snapshot()

for await (const data of snap.createBlockStream()) {
  console.log(data)
}

await snap.close()
