const BlockStream = require('./block-stream.js')
const { core, store } = require('./keys.js')
const b4a = require('b4a')

module.exports = {
  createBlockStream,
  createBitfieldStream,
  createUserDataStream,
  createDiscoveryKeyStream
}

function createDiscoveryKeyStream (db, updates) {
  const start = store.core(b4a.alloc(32))
  const end = store.core(b4a.alloc(32, 0xff))

  const ite = updates.iterator(db, start, end)

  ite._readableState.map = mapDiscoveryKey
  return ite
}

function createBlockStream (core, db, updates, start, end) {
  return new BlockStream(core, db, updates, start, end)
}

function createBitfieldStream (core, db, updates, start, end) {
  const s = core.bitfield(core.dataPointer, start, 0)
  const e = core.bitfield(core.dataPointer, end === -1 ? Infinity : end, 0)
  const ite = updates.iterator(db, s, e)

  ite._readableState.map = mapBitfield
  return ite
}

function createUserDataStream (core, db, updates, start, end) {
  const s = core.userData(core.dataPointer, start || '')
  const e = end === null ? core.userDataEnd(core.dataPointer) : core.userData(core.dataPointer, end)
  const ite = updates.iterator(db, s, e)

  ite._readableState.map = mapUserData
  return ite
}

function mapBitfield (data) {
  const [index, type] = core.bitfieldIndexAndType(data.key)
  if (type !== 0) return null // ignore for now
  return { index, value: data.value }
}

function mapUserData (data) {
  const key = core.userDataKey(data.key)
  return { key, value: data.value }
}

function mapDiscoveryKey (data) {
  const key = store.discoveryKey(data.key)
  return key
}
