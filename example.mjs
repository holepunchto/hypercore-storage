import RocksDB from 'rocksdb-native'
import b4a from 'b4a'
import { Readable } from 'streamx'
import BlockStream from './lib/block-stream.js'
import Updates from './lib/updates.js'
import { CorestoreTX, CorestoreRX, CoreTX, CoreRX } from './lib/tx.js'

const db = new RocksDB('/tmp/storage-2')

const core = {
  dataPointer: 10,
  corePointer: 11,
  dependencies: []
}

// db.iterator().on('data', console.log)


const u = new Updates(db)
const c = new CoreTX(core, db, u)

// c.putBlock(10, Buffer.from('hello'))
c.putBlock(9, Buffer.from('world'))
c.putTreeNode({ index: 10, size: 100, hash: Buffer.alloc(32) })
// c.deleteBlock(11)
// c.deleteBlockRange(0, 20)

const r = new CoreRX(core, db, u)

const a = r.getBlock(10)
const b = r.getBlock(11)

r.tryFlush()

console.log(await a)
console.log(await b)

// console.log(u)

const s = new BlockStream(core, db, u, 0, -1)

s.on('data', function (data) {
  console.log('block stream', data)
})
s.on('end', function () {
  console.log('ended...')
})

// await u.flush()

// const c = new CorestoreTX(db)

// c.setHead({
//   version: 0,
//   total: 3,
//   free: 0
// })

// const rx = new CorestoreRX(db, c.updates)

// const p = rx.getHead()

// rx.tryFlush()

// console.log(await p)

// await c.flush()
