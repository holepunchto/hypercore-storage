const schema = require('../spec/hyperschema')
const { store, core } = require('./keys.js')
const b4a = require('b4a')
const flat = require('flat-tree')

const CORESTORE_HEAD = schema.getEncoding('@corestore/head')
const CORESTORE_CORE = schema.getEncoding('@corestore/core')

const CORE_TREE_NODE = schema.getEncoding('@core/tree-node')
const CORE_AUTH = schema.getEncoding('@core/auth')

class CoreTX {
  constructor (core, db, updates) {
    this.core = core
    this.db = db
    this.updates = updates
  }

  setAuth (auth) {
    this.updates.put(core.auth(this.core.dataPointer), encode(CORE_AUTH, auth))
  }

  putBlock (index, data) {
    this.updates.put(core.block(this.core.dataPointer, index), data)
  }

  deleteBlock (index) {
    this.updates.delete(core.block(this.core.dataPointer, index))
  }

  deleteBlockRange (start, end) {
    this.updates.deleteRange(
      core.block(this.core.dataPointer, start),
      core.block(this.core.dataPointer, end === -1 ? Infinity : end)
    )
  }

  putBitfieldPage (index, data) {
    this.updates.put(core.bitfield(this.core.dataPointer, index, 0), data)
  }

  deleteBitfieldPage (index) {
    this.updates.delete(core.bitfield(this.core.dataPointer, index, 0))
  }

  putTreeNode (node) {
    this.updates.put(core.tree(this.core.dataPointer, node.index), encode(CORE_TREE_NODE, node))
  }

  deleteTreeNode (index) {
    this.updates.delete(core.tree(this.core.dataPointer, index))
  }

  deleteTreeNodeRange (start, end) {
    this.updates.deleteRange(
      core.tree(this.core.dataPointer, start),
      core.tree(this.core.dataPointer, end === -1 ? Infinity : end)
    )
  }

  putUserData (key, value) {
    this.updates.put(core.userData(this.core.dataPointer, key), value)
  }

  deleteUserData (key) {
    this.updates.delete(core.userData(this.core.dataPointer, key))
  }
}

class CoreRX {
  constructor (core, db, updates) {
    this.core = core
    this.read = db.read({ autoDestroy: true })
    this.updates = updates
  }

  getBlock (index) {
    const dep = findBlockDependency(this.core.dependencies, index)
    const data = dep === null ? this.core.dataPointer : dep.dataPointer
    return this.updates.get(this.read, core.block(data, index))
  }

  getBitfieldPage (index) {
    return this.updates.get(this.read, core.bitfield(this.core.dataPointer, index, 0))
  }

  async getTreeNode (index) {
    const dep = findTreeDependency(this.core.dependencies, index)
    const data = dep === null ? this.core.dataPointer : dep.dataPointer
    return decode(CORE_TREE_NODE, await this.updates.get(this.read, core.tree(data, index)))
  }

  getUserData (key) {
    return this.updates.get(this.read, core.userData(this.core.dataPointer, key))
  }

  tryFlush () {
    this.read.tryFlush()
  }

  destroy () {
    this.read.destroy()
  }
}

class CorestoreRX {
  constructor (db, updates) {
    this.read = db.read({ autoDestroy: true })
    this.updates = updates
  }

  async getHead () {
    return decode(CORESTORE_HEAD, await this.updates.get(this.read, store.head()))
  }

  async getCore (discoveryKey) {
    return decode(CORESTORE_CORE, await this.updates.get(this.read, store.core(discoveryKey)))
  }

  tryFlush () {
    this.read.tryFlush()
  }

  destroy () {
    this.read.destroy()
  }
}

class CorestoreTX {
  constructor (db, updates) {
    this.db = db
    this.updates = updates
  }

  setHead (head) {
    this.updates.put(store.head(), encode(CORESTORE_HEAD, head))
  }

  putCore (discoveryKey, ptr) {
    this.updates.put(store.core(discoveryKey), encode(CORESTORE_CORE, ptr))
  }

  clear () {
    const [start, end] = store.clear()
    this.updates.deleteRange(start, end)
  }
}

module.exports = { CorestoreTX, CorestoreRX, CoreTX, CoreRX }

function findBlockDependency (dependencies, index) {
  for (let i = 0; i < dependencies.length; i++) {
    const dep = dependencies[i]
    if (index < dep.length) return dep
  }

  return null
}

function findTreeDependency (dependencies, index) {
  for (let i = 0; i < dependencies.length; i++) {
    const dep = dependencies[i]
    if (flat.rightSpan(index) <= (dep.length - 1) * 2) return dep
  }

  return null
}

function decode (enc, buffer) {
  if (buffer === null) return null
  return enc.decode({ start: 0, end: buffer.byteLength, buffer })
}

function encode (enc, m) {
  // TODO: use fancy slab for small messages
  const state = { start: 0, end: 0, buffer: null }
  enc.preencode(state, m)
  state.buffer = b4a.allocUnsafe(state.end)
  enc.encode(state, m)
  return state.buffer
}
