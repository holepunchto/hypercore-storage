const RocksDB = require('rocksdb-native')
const c = require('compact-encoding')
const { UINT } = require('index-encoder')
const RW = require('read-write-mutexify')
const assert = require('nanoassert')
const m = require('./lib/messages')

const INF = Buffer.from([0xff])

// <TL_INFO> = { version, free, total }
// <TL_LOCAL_SEED> = seed
// <TL_CORE_INFO><discovery-key-32-bytes> = { version, owner, core, data }

// <core><CORE_MANIFEST>        = { key, manifest? }
// <core><CORE_LOCAL_SEED>      = seed
// <core><CORE_ENCRYPTION_KEY>  = encryptionKey // should come later, not important initially
// <core><CORE_HEAD><data>      = { fork, length, byteLength, signature }
// <core><CORE_BATCHES><name>   = <data>

// <data><CORE_INFO>            = { version }
// <data><CORE_UPDATES>         = { contiguousLength, blocks }
// <data><CORE_DEPENDENCY       = { data, length, roots }
// <data><CORE_HINTS>           = { reorg } // should come later, not important initially
// <data><CORE_TREE><index>     = { index, size, hash }
// <data><CORE_BITFIELD><index> = <4kb buffer>
// <data><CORE_BLOCKS><index>   = <buffer>
// <data><CORE_USER_DATA><key>  = <value>

// top level prefixes
const TL = {
  STORAGE_INFO: 0,
  LOCAL_SEED: 1,
  DKEYS: 2,
  CORE: 3,
  DATA: 4
}

// core prefixes
const CORE = {
  MANIFEST: 0,
  LOCAL_SEED: 1,
  ENCRYPTION_KEY: 2,
  HEAD: 3,
  BATCHES: 4
}

// data prefixes
const DATA = {
  INFO: 0,
  UPDATES: 1,
  DEPENDENCY: 2,
  HINTS: 3,
  TREE: 4,
  BITFIELD: 5,
  BLOCK: 6,
  USERDATA: 7
}

const SLAB = {
  start: 0,
  end: 65536,
  buffer: Buffer.allocUnsafe(65536)
}

// PREFIX + BATCH + TYPE + INDEX

class WriteBatch {
  constructor (storage, write) {
    this.storage = storage
    this.write = write
  }

  setHead (upgrade) {
    this.write.tryPut(encodeCoreIndex(this.storage.corePointer, CORE.HEAD), encode(m.CoreHead, upgrade))
  }

  putBlock (index, data) {
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index), data)
  }

  deleteBlock (index) {
    this.write.tryDelete(encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index))
  }

  deleteBlockRange (start, end) {
    return this._deleteRange(DATA.BLOCK, start, end)
  }

  putTreeNode (node) {
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.TREE, node.index), encode(m.TreeNode, node))
  }

  deleteTreeNode (index) {
    this.write.tryDelete(encodeDataIndex(this.storage.dataPointer, DATA.TREE, index))
  }

  deleteTreeNodeRange (start, end) {
    return this._deleteRange(DATA.TREE, start, end)
  }

  _deleteRange (type, start, end) {
    const s = encodeDataIndex(this.storage.dataPointer, type, start)
    const e = encodeDataIndex(this.storage.dataPointer, type, end === -1 ? Infinity : end)

    return this.write.deleteRange(s, e)
  }

  flush () {
    return this.write.flush()
  }
}

class ReadBatch {
  constructor (storage, read) {
    this.storage = storage
    this.read = read
  }

  async getHead () {
    return this._get(encodeCoreIndex(this.storage.corePointer, CORE.HEAD), m.CoreHead, false)
  }

  async hasBlock (index) {
    return this._has(encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index))
  }

  async getBlock (index, error) {
    const key = encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index)
    const block = await this._get(key, null, error)

    if (block === null && error === true) {
      throw new Error('Node not found: ' + index)
    }

    return block
  }

  async hasTreeNode (index) {
    return this._has(encodeDataIndex(this.storage.dataPointer, DATA.TREE, index))
  }

  async getTreeNode (index, error) {
    const key = encodeDataIndex(this.storage.dataPointer, DATA.TREE, index)
    const node = await this._get(key, m.TreeNode, error)

    if (node === null && error === true) {
      throw new Error('Node not found: ' + index)
    }

    return node
  }

  async _has (key) {
    return (await this.read.get(key)) !== null
  }

  async _get (key, enc) {
    const buffer = await this.read.get(key)
    if (buffer === null) return null

    if (enc) return c.decode(enc, buffer)

    return buffer
  }

  flush () {
    return this.read.flush()
  }

  tryFlush () {
    this.read.tryFlush()
  }
}

module.exports = class CoreStorage {
  constructor (dir) {
    this.db = new RocksDB(dir)
    this.mutex = new RW()
  }

  // just a helper to make tests easier
  static async clear (dir) {
    const s = new this(dir)
    await s.clear()
    return s
  }

  info () {
    return getStorageInfo(this.db)
  }

  list () {
    const s = this.db.iterator({
      gt: Buffer.from([TL.DKEYS]),
      lt: Buffer.from([TL.DKEYS + 1])
    })

    s._readableState.map = mapOnlyDiscoveryKey
    return s
  }

  ready () {
    return this.db.ready()
  }

  close () {
    return this.db.close()
  }

  async clear () {
    const b = this.db.write()
    b.tryDeleteRange(Buffer.from([TL.STORAGE_INFO]), INF)
    await b.flush()
  }

  get (discoveryKey) {
    return new HypercoreStorage(this.db, this.mutex, discoveryKey)
  }
}

class HypercoreStorage {
  constructor (db, mutex, discoveryKey) {
    this.db = db
    this.mutex = mutex
    this.discoveryKey = discoveryKey

    // pointers
    this.corePointer = -1
    this.dataPointer = -1
  }

  async open () {
    const val = await this.db.get(encodeDiscoveryKey(this.discoveryKey))
    if (val === null) return false

    const { core, data } = c.decode(m.CorePointer, val)

    this.corePointer = core
    this.dataPointer = data

    return true
  }

  async create ({ key, manifest, seed, encryptionKey, version }) {
    await this.mutex.write.lock()

    try {
      const existing = await this.open()

      if (existing) {
        // todo: verify key/manifest etc.
        return false
      }

      const write = this.db.write()
      const info = (await getStorageInfo(this.db)) || { free: 0, total: 0 }

      const core = info.total++
      const data = info.free++

      write.tryPut(encodeDiscoveryKey(this.discoveryKey), encode(m.CorePointer, { core, data }))
      write.tryPut(Buffer.from([TL.STORAGE_INFO]), encode(m.StorageInfo, info))

      this.corePointer = core
      this.dataPointer = data

      this.initialiseCoreInfo(write, { key, manifest, seed, encryptionKey })
      this.initialiseCoreData(write, { version })

      await write.flush()
    } finally {
      this.mutex.write.unlock()
    }

    return true
  }

  initialiseCoreInfo (db, { key, manifest, seed, encryptionKey }) {
    assert(this.corePointer >= 0)

    db.tryPut(encodeCoreIndex(this.corePointer, CORE.MANIFEST), encode(m.CoreAuth, { key, manifest }))
    // db.tryPut(encodeCoreIndex(this.corePointer, CORE.LOCAL_SEED), encode(m.CoreSeed, { seed }))
    // db.tryPut(encodeCoreIndex(this.corePointer, CORE.ENCRYPTION_KEY), encode(m.CoreEncryptionKey, { encryptionKey }))
  }

  initialiseCoreData (db, { version }) {
    assert(this.corePointer >= 0)

    db.tryPut(encodeCoreIndex(this.dataPointer, DATA.INFO), encode(m.DataInfo, { version }))
  }

  createReadBatch () {
    return new ReadBatch(this, this.db.read())
  }

  createWriteBatch () {
    return new WriteBatch(this, this.db.write())
  }

  createTreeNodeStream (opts = {}) {
    const r = encodeIndexRange(this.dataPointer, DATA.TREE, opts)
    const s = this.db.iterator(r)
    s._readableState.map = mapStreamTreeNode
    return s
  }

  getHead () {
    const b = this.createReadBatch()
    const p = b.getHead()
    b.tryFlush()
    return p
  }

  hasTreeNode (index) {
    const b = this.createReadBatch()
    const p = b.hasTreeNode(index)
    b.tryFlush()
    return p
  }

  getTreeNode (index, error) {
    const b = this.createReadBatch()
    const p = b.getTreeNode(index, error)
    b.tryFlush()
    return p
  }

  async peakLastTreeNode (opts = {}) {
    const last = await this.db.peek(encodeIndexRange(this.dataPointer, DATA.TREE, { reverse: true }))
    if (last === null) return null
    return c.decode(m.TreeNode, last.value)
  }

  close () {
    return this.db.close()
  }
}

function mapStreamTreeNode (data) {
  return c.decode(m.TreeNode, data.value)
}

function mapOnlyDiscoveryKey (data) {
  return data.key.subarray(1)
}

async function getStorageInfo (db) {
  const value = await db.get(Buffer.from([TL.STORAGE_INFO]))
  if (value === null) return null
  return c.decode(m.StorageInfo, value)
}

function ensureSlab (size) {
  if (SLAB.buffer.byteLength - SLAB.start < size) {
    SLAB.buffer = Buffer.allocUnsafe(SLAB.end)
    SLAB.start = 0
  }

  return SLAB
}

function encodeIndexRange (pointer, type, opts) {
  const bounded = { gt: null, gte: null, lte: null, lt: null, reverse: !!opts.reverse, limit: opts.limit || Infinity }

  if (opts.gt || opts.gt === 0) bounded.gt = encodeDataIndex(pointer, type, opts.gt)
  else if (opts.gte) bounded.gte = encodeDataIndex(pointer, type, opts.gte)
  else bounded.gte = encodeDataIndex(pointer, type, 0)

  if (opts.lt || opts.lt === 0) bounded.lt = encodeDataIndex(pointer, type, opts.lt)
  else if (opts.lte) bounded.lte = encodeDataIndex(pointer, type, opts.lte)
  else bounded.lte = encodeDataIndex(pointer, type, Infinity) // infinity

  return bounded
}

function encode (encoding, value) {
  const state = ensureSlab(128)
  const start = state.start
  encoding.encode(state, value)

  assert(state.start <= state.end)

  return state.buffer.subarray(start, state.start)
}

function encodeCoreIndex (pointer, type, index) {
  const state = ensureSlab(128)
  const start = state.start
  UINT.encode(state, TL.CORE)
  UINT.encode(state, pointer)
  UINT.encode(state, type)
  if (index !== undefined) UINT.encode(state, index)

  return state.buffer.subarray(start, state.start)
}

function encodeDataIndex (pointer, type, index) {
  const state = ensureSlab(128)
  const start = state.start
  UINT.encode(state, TL.DATA)
  UINT.encode(state, pointer)
  UINT.encode(state, type)
  if (index !== undefined) UINT.encode(state, index)

  return state.buffer.subarray(start, state.start)
}

function encodeDiscoveryKey (discoveryKey) {
  const state = ensureSlab(128)
  const start = state.start
  UINT.encode(state, TL.DKEYS)
  c.fixed32.encode(state, discoveryKey)
  return state.buffer.subarray(start, state.start)
}
