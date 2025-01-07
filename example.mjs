import RocksDB from 'rocksdb-native'
import Storage from './index.js'

const s = new Storage(new RocksDB('/tmp/my-corestore'))

// reset it first for simplicity
await s.clear()

console.log(await s.has(Buffer.alloc(32)))

const core2 = s.create({
  key: Buffer.alloc(32).fill('key2'),
  discoveryKey: Buffer.alloc(32).fill('discoveryKey2')
})
const core3 = s.create({
  key: Buffer.alloc(32).fill('key3'),
  discoveryKey: Buffer.alloc(32).fill('discoveryKey3')
})
const core2dup = s.create({
  key: Buffer.alloc(32).fill('key2'),
  discoveryKey: Buffer.alloc(32).fill('discoveryKey2')
})

console.log(await core2, await core3, await core2dup)

for await (const discoveryKey of s.list()) {
  console.log('has', discoveryKey)
}
