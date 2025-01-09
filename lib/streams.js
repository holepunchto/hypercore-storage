const BlockDependencyStream = require('./block-dependency-stream.js')
const { core, store } = require('./keys.js')
const schema = require('../spec/hyperschema')

const CORESTORE_CORE = schema.getEncoding('@corestore/core')

module.exports = {
  createBlockStream,
  createBitfieldStream,
  createUserDataStream,
  createCoreStream,
  createAliasStream
}

function createCoreStream (db, view) {
  const start = store.coreStart()
  const end = store.coreEnd()

  const ite = view.iterator(db, start, end, false)

  ite._readableState.map = mapCore
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

function createBlockStream (ptr, db, view, start, end, reverse) {
  const ite = createBlockIterator(ptr, db, view, start, end, reverse)

  ite._readableState.map = mapBlock
  return ite
}

function createBitfieldStream (ptr, db, view, start, end) {
  const s = core.bitfield(ptr.dataPointer, start, 0)
  const e = core.bitfield(ptr.dataPointer, end === -1 ? Infinity : end, 0)
  const ite = view.iterator(db, s, e, false)

  ite._readableState.map = mapBitfield
  return ite
}

function createUserDataStream (ptr, db, view, start, end) {
  const s = core.userData(ptr.dataPointer, start || '')
  const e = end === null ? core.userDataEnd(ptr.dataPointer) : core.userData(ptr.dataPointer, end)
  const ite = view.iterator(db, s, e, false)

  ite._readableState.map = mapUserData
  return ite
}

function mapBitfield (data) {
  const [index, type] = core.bitfieldIndexAndType(data.key)
  if (type !== 0) return null // ignore for now
  return { index, page: data.value }
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

function mapAlias (data) {
  const alias = store.alias(data.key)
  return { alias, discoveryKey: data.value }
}

function mapBlock (data) {
  return { index: core.blockIndex(data.key), value: data.value }
}
