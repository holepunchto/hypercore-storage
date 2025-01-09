const BlockStream = require('./block-stream.js')
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

function createCoreStream (db, updates) {
  const start = store.coreStart()
  const end = store.coreEnd()

  const ite = updates.iterator(db, start, end)

  ite._readableState.map = mapCore
  return ite
}

function createAliasStream (db, updates, namespace) {
  const start = store.coreByAliasStart(namespace)
  const end = store.coreByAliasEnd(namespace)

  const ite = updates.iterator(db, start, end)

  ite._readableState.map = mapAlias
  return ite
}

function createBlockStream (ptr, db, updates, start, end, reverse) {
  return new BlockStream(ptr, db, updates, start, end, reverse)
}

function createBitfieldStream (ptr, db, updates, start, end) {
  const s = core.bitfield(ptr.dataPointer, start, 0)
  const e = core.bitfield(ptr.dataPointer, end === -1 ? Infinity : end, 0)
  const ite = updates.iterator(db, s, e)

  ite._readableState.map = mapBitfield
  return ite
}

function createUserDataStream (ptr, db, updates, start, end) {
  const s = core.userData(ptr.dataPointer, start || '')
  const e = end === null ? core.userDataEnd(ptr.dataPointer) : core.userData(ptr.dataPointer, end)
  const ite = updates.iterator(db, s, e)

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
