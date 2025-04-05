const schema = require('../spec/hyperschema')
const { store, core } = require('./keys.js')
const View = require('./view.js')
const b4a = require('b4a')
const flat = require('flat-tree')

const CORESTORE_HEAD = schema.getEncoding('@corestore/head')
const CORESTORE_CORE = schema.getEncoding('@corestore/core')

const CORE_AUTH = schema.getEncoding('@core/auth')
const CORE_SESSIONS = schema.getEncoding('@core/sessions')
const CORE_HEAD = schema.getEncoding('@core/head')
const CORE_TREE_NODE = schema.getEncoding('@core/tree-node')
const CORE_DEPENDENCY = schema.getEncoding('@core/dependency')
const CORE_HINTS = schema.getEncoding('@core/hints')

class CoreTX {
  constructor (core, db, view, changes) {
    if (db.snapshotted) throw new Error('Cannot open core tx on snapshot')
    this.core = core
    this.db = db
    this.view = view
    this.changes = changes
  }

  setAuth (auth) {
    this.changes.push([core.auth(this.core.corePointer), encode(CORE_AUTH, auth), null])
  }

  setSessions (sessions) {
    this.changes.push([core.sessions(this.core.corePointer), encode(CORE_SESSIONS, sessions), null])
  }

  setHead (head) {
    this.changes.push([core.head(this.core.dataPointer), encode(CORE_HEAD, head), null])
  }

  deleteHead () {
    this.changes.push([core.head(this.core.dataPointer), null, null])
  }

  setDependency (dep) {
    this.changes.push([core.dependency(this.core.dataPointer), encode(CORE_DEPENDENCY, dep), null])
  }

  setHints (hints) {
    this.changes.push([core.hints(this.core.dataPointer), encode(CORE_HINTS, hints), null])
  }

  putBlock (index, data) {
    this.changes.push([core.block(this.core.dataPointer, index), data, null])
  }

  deleteBlock (index) {
    this.changes.push([core.block(this.core.dataPointer, index), null, null])
  }

  deleteBlockRange (start, end) {
    this.changes.push([
      core.block(this.core.dataPointer, start),
      null,
      core.block(this.core.dataPointer, end === -1 ? Infinity : end)
    ])
  }

  putBitfieldPage (index, data) {
    this.changes.push([core.bitfield(this.core.dataPointer, index, 0), data, null])
  }

  deleteBitfieldPage (index) {
    this.changes.push([core.bitfield(this.core.dataPointer, index, 0), null, null])
  }

  deleteBitfieldPageRange (start, end) {
    this.changes.push([
      core.bitfield(this.core.dataPointer, start, 0),
      null,
      core.bitfield(this.core.dataPointer, end === -1 ? Infinity : end, 0)
    ])
  }

  putTreeNode (node) {
    this.changes.push([core.tree(this.core.dataPointer, node.index), encode(CORE_TREE_NODE, node), null])
  }

  deleteTreeNode (index) {
    this.changes.push([core.tree(this.core.dataPointer, index), null, null])
  }

  deleteTreeNodeRange (start, end) {
    this.changes.push([
      core.tree(this.core.dataPointer, start),
      null,
      core.tree(this.core.dataPointer, end === -1 ? Infinity : end)
    ])
  }

  putUserData (key, value) {
    const buffer = typeof value === 'string' ? b4a.from(value) : value
    this.changes.push([core.userData(this.core.dataPointer, key), buffer, null])
  }

  deleteUserData (key) {
    this.changes.push([core.userData(this.core.dataPointer, key), null, null])
  }

  putLocal (key, value) {
    this.changes.push([core.local(this.core.dataPointer, key), value, null])
  }

  deleteLocal (key) {
    this.changes.push([core.local(this.core.dataPointer, key), null, null])
  }

  deleteLocalRange (start, end) {
    this.changes.push([
      core.local(this.core.dataPointer, start),
      null,
      end === null ? core.localEnd(this.core.dataPointer) : core.local(this.core.dataPointer, end)
    ])
  }

  flush () {
    const changes = this.changes
    if (changes === null) return Promise.resolve(!this.view)

    this.changes = null

    if (this.view) {
      this.view.apply(changes)
      return Promise.resolve(false)
    }

    return View.flush(changes, this.db)
  }
}

class CoreRX {
  constructor (core, db, view) {
    this.core = core
    this.read = db.read({ autoDestroy: true })
    this.view = view

    view.readStart()
  }

  async getAuth () {
    return await decode(CORE_AUTH, await this.view.get(this.read, core.auth(this.core.corePointer)))
  }

  async getSessions () {
    return await decode(CORE_SESSIONS, await this.view.get(this.read, core.sessions(this.core.corePointer)))
  }

  async getHead () {
    return await decode(CORE_HEAD, await this.view.get(this.read, core.head(this.core.dataPointer)))
  }

  async getDependency () {
    return await decode(CORE_DEPENDENCY, await this.view.get(this.read, core.dependency(this.core.dataPointer)))
  }

  async getHints () {
    return await decode(CORE_HINTS, await this.view.get(this.read, core.hints(this.core.dataPointer)))
  }

  getBlock (index) {
    const dep = findBlockDependency(this.core.dependencies, index)
    const data = dep === null ? this.core.dataPointer : dep.dataPointer
    return this.view.get(this.read, core.block(data, index))
  }

  getBitfieldPage (index) {
    return this.view.get(this.read, core.bitfield(this.core.dataPointer, index, 0))
  }

  async getTreeNode (index) {
    const dep = findTreeDependency(this.core.dependencies, index)
    const data = dep === null ? this.core.dataPointer : dep.dataPointer
    return decode(CORE_TREE_NODE, await this.view.get(this.read, core.tree(data, index)))
  }

  async hasTreeNode (index) {
    return (await this.getTreeNode(index)) !== null
  }

  getUserData (key) {
    return this.view.get(this.read, core.userData(this.core.dataPointer, key))
  }

  getLocal (key) {
    return this.view.get(this.read, core.local(this.core.dataPointer, key))
  }

  tryFlush () {
    this.read.tryFlush()
    this._free()
  }

  destroy () {
    this.read.destroy()
    this._free()
  }

  _free () {
    if (this.view === null) return
    this.view.readStop()
    this.view = null
  }
}

class CorestoreTX {
  constructor (view) {
    this.view = view
    this.changes = []
  }

  setHead (head) {
    this.changes.push([store.head(), encode(CORESTORE_HEAD, head), null])
  }

  putCore (discoveryKey, ptr) {
    this.changes.push([store.core(discoveryKey), encode(CORESTORE_CORE, ptr), null])
  }

  putCoreByAlias (alias, discoveryKey) {
    this.changes.push([store.coreByAlias(alias), discoveryKey, null])
  }

  clear () {
    const [start, end] = store.clear()
    this.changes.push([start, null, end])
  }

  apply () {
    if (this.changes === null) return
    this.view.apply(this.changes)
    this.changes = null
  }
}

class CorestoreRX {
  constructor (db, view) {
    this.read = db.read({ autoDestroy: true })
    this.view = view

    view.readStart()
  }

  async getHead () {
    return decode(CORESTORE_HEAD, await this.view.get(this.read, store.head()))
  }

  async getCore (discoveryKey) {
    return decode(CORESTORE_CORE, await this.view.get(this.read, store.core(discoveryKey)))
  }

  getCoreByAlias (alias) {
    return this.view.get(this.read, store.coreByAlias(alias))
  }

  tryFlush () {
    this.read.tryFlush()
    this._free()
  }

  destroy () {
    this.read.destroy()
    this._free()
  }

  _free () {
    if (this.view === null) return
    this.view.readStop()
    this.view = null
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
