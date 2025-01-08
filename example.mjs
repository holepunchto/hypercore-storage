import RocksDB from 'rocksdb-native'
import Storage from './index.js'

const s = new Storage(new RocksDB('/tmp/my-corestore'))

// reset it first for simplicity
await s.clear()

const core = await s.create({
  key: Buffer.alloc(32).fill('key'),
  discoveryKey: Buffer.alloc(32).fill('discoveryKey'),
  alias: {
    namespace: Buffer.alloc(32).fill('namespace'),
    name: 'hello'
  }
})

const dk = await s.getAlias({
  namespace: Buffer.alloc(32).fill('namespace'),
  name: 'hello'
})

console.log('-->', dk)

const tx = core.createWriteBatch()
const batch = await core.createBatch(tx, 'test', null)

const a = await core.resumeBatch(null, 'test')
const b = await core.resumeBatch(tx, 'test')

console.log(tx.updates, !!a, !!b)

for await (const result of s.createAliasStream()) {
  console.log(result)
}
