const b4a = require('b4a')
const BlockDependencyStream = require('./block-dependency-stream.js')
const { core, store } = require('./keys.js')
const schema = require('../spec/hyperschema')

const CORESTORE_CORE = schema.getEncoding('@corestore/core')
const CORE_TREE_NODE = schema.getEncoding('@core/tree-node')
const EMPTY = b4a.alloc(0)

module.exports = {
  createBlockStream,
  createBitfieldStream,
  createUserDataStream,
  createCoreStream,
  createAliasStream,
  createDiscoveryKeyStream,
  createTreeNodeStream,
  createLocalStream
}

function createCoreStream (db, view) {
  const start = store.coreStart()
  const end = store.coreEnd()

  const ite = view.iterator(db, start, end, false)

  ite._readableState.map = mapCore
  return ite
}

function createDiscoveryKeyStream (db, view, namespace) {
  const start = namespace ? store.coreByAliasStart(namespace) : store.coreStart()
  const end = namespace ? store.coreByAliasEnd(namespace) : store.coreEnd()

  const ite = view.iterator(db, start, end, false)

  ite._readableState.map = namespace ? mapNamespaceDiscoveryKeys : mapAllDiscoveryKeys
  return ite
}

function createAliasStream (db, view, namespace) {
  const start = store.coreByAliasStart(namespace)
  const end = store.coreByAliasEnd(namespace)

  const ite = view.iterator(db, start, end, false)

  ite._readableState.map = mapAlias
  return ite
}

function createBlockIterator (ptr, db, view, start, end, reverse) {
  if (ptr.dependencies.length > 0) {
    return new BlockDependencyStream(ptr, db, view, start, end, reverse)
  }

  const s = core.block(ptr.dataPointer, start)
  const e = core.block(ptr.dataPointer, end === -1 ? Infinity : end)
  return view.iterator(db, s, e, reverse)
}

function createBlockStream (ptr, db, view, { gt = -1, gte = gt + 1, lte = -1, lt = lte === -1 ? -1 : lte + 1, reverse = false } = {}) {
  const ite = createBlockIterator(ptr, db, view, gte, lt, reverse)

  ite._readableState.map = mapBlock
  return ite
}

function createBitfieldStream (ptr, db, view, { gt = -1, gte = gt + 1, lte = -1, lt = lte === -1 ? -1 : lte + 1, reverse = false } = {}) {
  const s = core.bitfield(ptr.dataPointer, gte, 0)
  const e = core.bitfield(ptr.dataPointer, lt === -1 ? Infinity : lt, 0)
  const ite = view.iterator(db, s, e, false)

  ite._readableState.map = mapBitfield
  return ite
}

// NOTE: this does not do dependency lookups atm
function createTreeNodeStream (ptr, db, view, { gt = -1, gte = gt + 1, lte = -1, lt = lte === -1 ? -1 : lte + 1, reverse = false } = {}) {
  const s = core.tree(ptr.dataPointer, gte, 0)
  const e = core.tree(ptr.dataPointer, lt === -1 ? Infinity : lt, 0)
  const ite = view.iterator(db, s, e, false)

  ite._readableState.map = mapTreeNode
  return ite
}

function createUserDataStream (ptr, db, view, { gt = null, gte = '', lte = null, lt = null, reverse = false } = {}) {
  if (gt !== null || lte !== null) throw new Error('gt and lte not yet supported for user data streams')

  const s = core.userData(ptr.dataPointer, gte)
  const e = lt === null ? core.userDataEnd(ptr.dataPointer) : core.userData(ptr.dataPointer, lt)
  const ite = view.iterator(db, s, e, false)

  ite._readableState.map = mapUserData
  return ite
}

function createLocalStream (ptr, db, view, { gt = null, gte = EMPTY, lte = null, lt = null, reverse = false } = {}) {
  if (gt !== null || lte !== null) throw new Error('gt and lte not yet supported for local streams')

  const s = core.local(ptr.dataPointer, gte)
  const e = lt === null ? core.localEnd(ptr.dataPointer) : core.local(ptr.dataPointer, lt)
  const ite = view.iterator(db, s, e, false)

  ite._readableState.map = mapLocal
  return ite
}

function mapBitfield (data) {
  const [index, type] = core.bitfieldIndexAndType(data.key)
  if (type !== 0) return null // ignore for now
  return { index, page: data.value }
}

function mapLocal (data) {
  const key = core.localKey(data.key)
  return { key, value: data.value }
}

function mapUserData (data) {
  const key = core.userDataKey(data.key)
  return { key, value: data.value }
}

function mapCore (data) {
  const discoveryKey = store.discoveryKey(data.key)
  const core = CORESTORE_CORE.decode({ start: 0, end: data.value.byteLength, buffer: data.value })
  return { discoveryKey, core }
}

function mapAllDiscoveryKeys (data) {
  return store.discoveryKey(data.key)
}

function mapNamespaceDiscoveryKeys (data) {
  return data.value
}

function mapAlias (data) {
  const alias = store.alias(data.key)
  return { alias, discoveryKey: data.value }
}

function mapBlock (data) {
  return { index: core.blockIndex(data.key), value: data.value }
}

function mapTreeNode (data) {
  return CORE_TREE_NODE.decode({ start: 0, end: data.value.byteLength, buffer: data.value })
}
