const RocksDB = require('rocksdb-native')
const c = require('compact-encoding')
const { UINT } = require('index-encoder')

const EMPTY = Buffer.alloc(0)

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
    // TODO: add tryAdd to rocks
    this.batch.add(encodeIndex(node.index), encodeTreeNode(node)).catch(noop)
  }

  flush () {
    return this.batch.write()
  }
}

class ReadBatch {
  constructor (batch) {
    this.batch = batch
  }

  async getTreeNode (index) {
    return decodeTreeNode(await this.batch.add(encodeIndex(index), EMPTY))
  }

  flush () {
    return this.batch.read()
  }

  tryFlush () {
    // TODO: add tryFlush to rocks
    this.flush().catch(noop)
  }
}

module.exports = class RocksStorage {
  constructor (dir) {
    this.db = new RocksDB(dir)
  }

  createReadBatch () {
    return new ReadBatch(this.db.batch())
  }

  createWriteBatch () {
    return new WriteBatch(this.db.batch())
  }

  getTreeNode (index) {
    const b = this.createReadBatch()
    const p = b.getTreeNode(index)
    b.tryFlush()
    return p
  }

  close () {
    return this.db.close()
  }
}

function ensureSmallSlab () {
  if (SMALL_SLAB.buffer.byteLength - SMALL_SLAB.start < 64) {
    SMALL_SLAB.buffer = Buffer.allocUnsafe(SMALL_SLAB.end)
    SMALL_SLAB.start = 0
  }

  return SMALL_SLAB
}

function encodeIndex (index) {
  const state = ensureSmallSlab()
  const start = state.start
  UINT.encode(state, index)
  return state.buffer.subarray(start, state.start)
}

function decodeTreeNode (buffer) {
  if (buffer === null) return null

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

function noop () {}
