const BlockStream = require('./block-stream.js')
const { core } = require('./keys.js')

module.exports = {
  createBlockStream,
  createBitfieldStream,
  createUserDataStream
}

function createBlockStream (core, db, updates, start, end) {
  return new BlockStream(core, db, updates, start, end)
}

function createBitfieldStream (core, db, updates, start, end) {
  const s = core.bitfield(core.dataPointer, start, 0)
  const e = core.bitfield(core.dataPointer, end === -1 ? Infinity : end, 0)
  const ite = updates.iterator(db, s, e)

  ite._readableState.map = mapBitfield
}

function createUserDataStream (core, db, updates, start, end) {
  const s = core.userData(core.dataPointer, start || '')
  const e = end === null ? core.userDataEnd(core.dataPointer) : core.userData(core.dataPointer, end)
  const ite = updates.iterator(db, s, e)

  ite._readableState.map = mapUserData
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
