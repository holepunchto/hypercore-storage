const RocksDB = require('rocksdb-native')
const c = require('compact-encoding')
const { UINT } = require('index-encoder')
const m = require('./lib/messages')

const INF = Buffer.from([0xff])
const META = Buffer.from([0x00])
const DKEYS = Buffer.from([0x01])

const CORE_META = 0
const CORE_TREE = 1

const SLAB = {
  start: 0,
  end: 65536,
  buffer: Buffer.allocUnsafe(65536)
}

// PREFIX + BATCH + TYPE + INDEX

class WriteBatch {
  constructor (storage, batch) {
    this.storage = storage
    this.batch = batch
  }

  setUpgrade (batch, upgrade) {
    this.batch.tryPut(encodeBatchIndex(this.storage.authPrefix, batch, CORE_META, 0), encodeUpgrade(upgrade))
  }

  addTreeNode (batch, node) {
    this.batch.tryPut(encodeBatchIndex(this.storage.dataPrefix, batch, CORE_TREE, node.index), encodeTreeNode(node))
  }

  deleteTreeNode (batch, index) {
    this.batch.tryDelete(encodeBatchIndex(this.storage.dataPrefix, batch, CORE_TREE, index))
  }

  deleteTreeNodeRange (batch, start, end) {
    const s = encodeBatchIndex(this.storage.dataPrefix, batch, CORE_TREE, start)
    const e = encodeBatchIndex(this.storage.dataPrefix, batch, CORE_TREE, end)

    return this.batch.deleteRange(s, e)
  }

  flush () {
    return this.batch.flush()
  }
}

class ReadBatch {
  constructor (storage, batch) {
    this.storage = storage
    this.batch = batch
  }

  async getUpgrade (batch) {
    const buffer = await this.batch.get(encodeBatchIndex(this.storage.authPrefix, batch, CORE_META, 0))
    return buffer === null ? null : decodeUpgrade(buffer)
  }

  async hasTreeNode (batch, index) {
    return (await this.batch.get(encodeBatchIndex(this.storage.dataPrefix, batch, CORE_TREE, index))) !== null
  }

  async getTreeNode (batch, index, error) {
    const buffer = await this.batch.get(encodeBatchIndex(this.storage.dataPrefix, batch, CORE_TREE, index))

    if (buffer === null) {
      if (error === true) throw new Error('Node not found: ' + index)
      return null
    }

    return decodeTreeNode(buffer)
  }

  flush () {
    return this.batch.flush()
  }

  tryFlush () {
    this.batch.tryFlush()
  }
}

module.exports = class CoreStorage {
  constructor (dir) {
    this.db = new RocksDB(dir)
  }

  // just a helper to make tests easier
  static async clear (dir) {
    const s = new this(dir)
    await s.clear()
    return s
  }

  list () {
    const s = this.db.iterator({
      gt: DKEYS,
      lt: Buffer.from([DKEYS[0] + 1])
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
    b.tryDeleteRange(META, INF)
    await b.flush()
  }

  get (discoveryKey) {
    return new HypercoreStorage(this.db, discoveryKey)
  }
}

class HypercoreStorage {
  constructor (db, discoveryKey) {
    this.db = db
    this.discoveryKey = discoveryKey
    this.authPrefix = null
    this.dataPrefix = null
  }

  async open () {
    const val = await this.db.get(Buffer.concat([DKEYS, this.discoveryKey]))
    if (val === null) return false
    this._onopen(c.decode(m.DiscoveryKey, val))
    return true
  }

  async create ({ key, manifest, localKeyPair }) {
    const b = this.db.write()
    const auth = 16 // TODO
    const data = 16 // TODO
    b.tryPut(Buffer.concat([DKEYS, this.discoveryKey]), c.encode(m.DiscoveryKey, { auth, data }))
    await b.flush()
    this._onopen({ auth, data })
  }

  _onopen ({ auth, data }) {
    this.authPrefix = c.encode(UINT, auth)
    this.dataPrefix = c.encode(UINT, data)
  }

  createReadBatch () {
    return new ReadBatch(this, this.db.read())
  }

  createWriteBatch () {
    return new WriteBatch(this, this.db.write())
  }

  createTreeNodeStream (batch, opts = {}) {
    const r = encodeIndexRange(encodeBatchPrefix(this.dataPrefix, batch, CORE_TREE), opts)
    const s = this.db.iterator(r)
    s._readableState.map = mapStreamTreeNode
    return s
  }

  getUpgrade (batch) {
    const b = this.createReadBatch()
    const p = b.getUpgrade(batch)
    b.tryFlush()
    return p
  }

  hasTreeNode (batch, index) {
    const b = this.createReadBatch()
    const p = b.hasTreeNode(index)
    b.tryFlush()
    return p
  }

  getTreeNode (batch, index, error) {
    const b = this.createReadBatch()
    const p = b.getTreeNode(index, error)
    b.tryFlush()
    return p
  }

  async peakLastTreeNode (batch, opts = {}) {
    const last = await this.db.peek(encodeIndexRange(encodeBatchPrefix(this.dataPrefix, batch, CORE_TREE), { reverse: true }))
    if (last === null) return null
    return decodeTreeNode(last.value)
  }

  close () {
    return this.db.close()
  }
}

function mapStreamTreeNode (data) {
  return decodeTreeNode(data.value)
}

function mapOnlyDiscoveryKey (data) {
  return data.key.subarray(1)
}

function ensureSlab (size) {
  if (SLAB.buffer.byteLength - SLAB.start < size) {
    SLAB.buffer = Buffer.allocUnsafe(SLAB.end)
    SLAB.start = 0
  }

  return SLAB
}

function encodeIndexRange (prefix, opts) {
  const bounded = { gt: null, gte: null, lte: null, lt: null, reverse: !!opts.reverse, limit: opts.limit || Infinity }

  if (opts.gt || opts.gt === 0) bounded.gt = encodeIndex(prefix, opts.gt)
  else if (opts.gte) bounded.gte = encodeIndex(prefix, opts.gte)
  else bounded.gte = encodeIndex(prefix, 0)

  if (opts.lt || opts.lt === 0) bounded.lt = encodeIndex(prefix, opts.lt)
  else if (opts.lte) bounded.lte = encodeIndex(prefix, opts.lte)
  else bounded.lte = Buffer.concat([prefix, INF]) // infinity

  return bounded
}

function encodeBatchPrefix (prefix, batchId, type) {
  const state = ensureSlab(128)
  const start = state.start
  state.buffer.set(prefix, start)
  state.start += prefix.byteLength
  UINT.encode(state, batchId)
  UINT.encode(state, type)
  return state.buffer.subarray(start, state.start)
}

function encodeBatchIndex (prefix, batchId, type, index) {
  const state = ensureSlab(128)
  const start = state.start
  state.buffer.set(prefix, start)
  state.start += prefix.byteLength
  UINT.encode(state, batchId)
  UINT.encode(state, type)
  UINT.encode(state, index)
  return state.buffer.subarray(start, state.start)
}

function encodeIndex (prefix, index) {
  const state = ensureSlab(128)
  const start = state.start
  state.buffer.set(prefix, start)
  state.start += prefix.byteLength
  UINT.encode(state, index)
  return state.buffer.subarray(start, state.start)
}

function decodeTreeNode (buffer) {
  return m.TreeNode.decode({ start: 0, end: buffer.byteLength, buffer })
}

function encodeTreeNode (node) {
  const state = ensureSlab(64)
  const start = state.start
  m.TreeNode.encode(state, node)
  return state.buffer.subarray(start, state.start)
}

function encodeUpgrade (upgrade) {
  const state = ensureSlab(128)
  const start = state.start
  m.Upgrade.encode(state, upgrade)
  return state.buffer.subarray(start, state.start)
}

function decodeUpgrade (buffer) {
  return m.Upgrade.decode({ start: 0, end: buffer.byteLength, buffer })
}
