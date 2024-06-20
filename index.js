const RocksDB = require('rocksdb-native')
const c = require('compact-encoding')
const { UINT } = require('index-encoder')
const m = require('./lib/messages')

const INF = Buffer.from([0xff])
const DKEYS = Buffer.from([0x1])

const SMALL_SLAB = {
  start: 0,
  end: 65536,
  buffer: Buffer.allocUnsafe(65536)
}

class WriteBatch {
  constructor (storage, batch) {
    this.storage = storage
    this.batch = batch
  }

  addTreeNode (node) {
    this.batch.tryPut(encodeIndex(this.storage.dataPrefix, node.index), encodeTreeNode(node))
  }

  deleteTreeNode (index) {
    this.batch.tryDelete(encodeIndex(this.storage.dataPrefix, index))
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

  async hasTreeNode (index) {
    return (await this.batch.get(encodeIndex(this.storage.dataPrefix, index))) !== null
  }

  async getTreeNode (index, error) {
    const buffer = await this.batch.get(encodeIndex(this.storage.dataPrefix, index))

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

  get (discoveryKey) {
    return new HypercoreStorage(this.db, discoveryKey)
  }
}

class HypercoreStorage {
  constructor (db, discoveryKey) {
    this.db = db
    this.discoveryKey = discoveryKey
    this.headerPrefix = null
    this.dataPrefix = null
  }

  async open () {
    const b = this.db.read()
    const p = b.get(Buffer.concat([DKEYS, this.discoveryKey]))
    b.tryFlush()
    const val = await p
    if (val === null) return false
    this._onopen(c.decode(m.DiscoveryKey, val))
    return true
  }

  async create () {
    const b = this.db.write()
    const header = 16 // TODO
    const data = 16 // TODO
    b.tryPut(Buffer.concat([DKEYS, this.discoveryKey]), c.encode(m.DiscoveryKey, { header, data }))
    await b.flush()
    this._onopen({ header, data })
  }

  _onopen ({ header, data }) {
    this.headerPrefix = c.encode(UINT, header)
    this.dataPrefix = c.encode(UINT, data)
  }

  createReadBatch () {
    return new ReadBatch(this, this.db.read())
  }

  createWriteBatch () {
    return new WriteBatch(this, this.db.write())
  }

  createTreeNodeStream (opts = {}) {
    const r = encodeIndexRange(this.dataPrefix, opts)
    const s = this.db.iterator(r)
    s._readableState.map = mapStreamTreeNode
    return s
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
    let last = null
    for await (const data of this.createTreeNodeStream({ reverse: true, limit: 1 })) {
      last = data
    }
    return last
  }

  close () {
    return this.db.close()
  }
}

function mapStreamTreeNode (data) {
  return decodeTreeNode(data.value)
}

function ensureSmallSlab () {
  if (SMALL_SLAB.buffer.byteLength - SMALL_SLAB.start < 128) {
    SMALL_SLAB.buffer = Buffer.allocUnsafe(SMALL_SLAB.end)
    SMALL_SLAB.start = 0
  }

  return SMALL_SLAB
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

function encodeIndex (prefix, index) {
  const state = ensureSmallSlab()
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
  const state = ensureSmallSlab()
  const start = state.start
  m.TreeNode.encode(state, node)
  return state.buffer.subarray(start, state.start)
}
