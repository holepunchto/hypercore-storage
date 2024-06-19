const RocksDB = require('rocksdb-native')
const c = require('compact-encoding')
const { UINT } = require('index-encoder')

const SMALL_SLAB = {
  start: 0,
  end: 65536,
  buffer: Buffer.allocUnsafe(65536)
}

class WriteBatch {
  constructor (batch) {
    this.batch = batch
  }

  addTreeNode (node) {
    this.batch.tryPut(encodeIndex(node.index), encodeTreeNode(node))
  }

  deleteTreeNode (index) {
    this.batch.tryDelete(encodeIndex(index))
  }

  flush () {
    return this.batch.flush()
  }
}

class ReadBatch {
  constructor (batch) {
    this.batch = batch
  }

  async hasTreeNode (index) {
    return (await this.batch.get(encodeIndex(index))) !== null
  }

  async getTreeNode (index, error) {
    const buffer = await this.batch.get(encodeIndex(index))

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

module.exports = class RocksStorage {
  constructor (dir) {
    this.db = new RocksDB(dir)
  }

  createReadBatch () {
    return new ReadBatch(this.db.write())
  }

  createWriteBatch () {
    return new WriteBatch(this.db.read())
  }

  createTreeNodeStream (opts = {}) {
    const r = encodeIndexRange(opts)
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

  async peakTreeNode (opts = {}) {
    let last = null
    for await (const data of this.createTreeNodeStream({ ...opts, limit: 1 })) {
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
  if (SMALL_SLAB.buffer.byteLength - SMALL_SLAB.start < 64) {
    SMALL_SLAB.buffer = Buffer.allocUnsafe(SMALL_SLAB.end)
    SMALL_SLAB.start = 0
  }

  return SMALL_SLAB
}

function encodeIndexRange (opts) {
  const bounded = { gt: null, gte: null, lte: null, lt: null, reverse: !!opts.reverse, limit: opts.limit || Infinity }

  if (opts.gt || opts.gt === 0) bounded.gt = encodeIndex(opts.gt)
  else if (opts.gte) bounded.gte = encodeIndex(opts.gte)
  else bounded.gte = encodeIndex(0)

  if (opts.lt || opts.lt === 0) bounded.lt = encodeIndex(opts.lt)
  else if (opts.lte) bounded.lte = encodeIndex(opts.lte)
  else bounded.lte = Buffer.from([0xff]) // infinity

  return bounded
}

function encodeIndex (index) {
  const state = ensureSmallSlab()
  const start = state.start
  UINT.encode(state, index)
  return state.buffer.subarray(start, state.start)
}

function decodeTreeNode (buffer) {
  const state = { start: 0, end: buffer.byteLength, buffer }

  return {
    index: c.uint.decode(state),
    size: c.uint.decode(state),
    hash: c.fixed32.decode(state)
  }
}

function encodeTreeNode (node) {
  const state = ensureSmallSlab()
  const start = state.start
  c.uint.encode(state, node.index)
  c.uint.encode(state, node.size)
  c.fixed32.encode(state, node.hash)
  return state.buffer.subarray(start, state.start)
}
