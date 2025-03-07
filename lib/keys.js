const { UINT, STRING } = require('index-encoder')
const c = require('compact-encoding')
const b4a = require('b4a')

const TL_HEAD = 0
const TL_CORE_BY_DKEY = 1
const TL_CORE_BY_ALIAS = 2
const TL_CORE = 3
const TL_DATA = 4

const TL_END = TL_DATA + 1

const CORE_AUTH = 0
const CORE_SESSIONS = 1

const DATA_HEAD = 0
const DATA_DEPENDENCY = 1
const DATA_HINTS = 2
const DATA_BLOCK = 3
const DATA_TREE = 4
const DATA_BITFIELD = 5
const DATA_USER_DATA = 6
const DATA_LOCAL = 7

const slab = { buffer: b4a.allocUnsafe(65536), start: 0, end: 0 }

const store = {}
const core = {}

store.clear = function () {
  const state = alloc()
  let start = state.start
  UINT.encode(state, 0)
  const a = state.buffer.subarray(start, state.start)
  start = state.start
  UINT.encode(state, TL_END)
  const b = state.buffer.subarray(start, state.start)
  return [a, b]
}

store.head = function () {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_HEAD)
  return state.buffer.subarray(start, state.start)
}

store.core = function (discoveryKey) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE_BY_DKEY)
  c.fixed32.encode(state, discoveryKey)
  return state.buffer.subarray(start, state.start)
}

store.coreStart = function () {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE_BY_DKEY)
  return state.buffer.subarray(start, state.start)
}

store.coreEnd = function () {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE_BY_DKEY + 1)
  return state.buffer.subarray(start, state.start)
}

store.coreByAlias = function ({ namespace, name }) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE_BY_ALIAS)
  c.fixed32.encode(state, namespace)
  STRING.encode(state, name)
  return state.buffer.subarray(start, state.start)
}

store.coreByAliasStart = function (namespace) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE_BY_ALIAS)
  if (namespace) c.fixed32.encode(state, namespace)
  return state.buffer.subarray(start, state.start)
}

store.coreByAliasEnd = function (namespace) {
  const state = alloc()
  const start = state.start

  if (namespace) {
    UINT.encode(state, TL_CORE_BY_ALIAS)
    c.fixed32.encode(state, namespace)
    state.buffer[state.start++] = 0xff
  } else {
    UINT.encode(state, TL_CORE_BY_ALIAS + 1)
  }

  return state.buffer.subarray(start, state.start)
}

store.alias = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  const namespace = c.fixed32.decode(state)
  const name = STRING.decode(state)
  return { namespace, name }
}

store.discoveryKey = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  return c.fixed32.decode(state)
}

core.core = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE)
  UINT.encode(state, ptr)
  return state.buffer.subarray(start, state.start)
}

core.data = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  return state.buffer.subarray(start, state.start)
}

core.auth = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE)
  UINT.encode(state, ptr)
  UINT.encode(state, CORE_AUTH)
  return state.buffer.subarray(start, state.start)
}

core.sessions = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE)
  UINT.encode(state, ptr)
  UINT.encode(state, CORE_SESSIONS)
  return state.buffer.subarray(start, state.start)
}

core.head = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_HEAD)
  return state.buffer.subarray(start, state.start)
}

core.dependency = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_DEPENDENCY)
  return state.buffer.subarray(start, state.start)
}

core.hints = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_HINTS)
  return state.buffer.subarray(start, state.start)
}

core.block = function (ptr, index) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_BLOCK)
  UINT.encode(state, index)
  return state.buffer.subarray(start, state.start)
}

core.tree = function (ptr, index) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_TREE)
  UINT.encode(state, index)
  return state.buffer.subarray(start, state.start)
}

core.bitfield = function (ptr, index, type) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_BITFIELD)
  UINT.encode(state, index)
  UINT.encode(state, type)
  return state.buffer.subarray(start, state.start)
}

core.userData = function (ptr, key) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_USER_DATA)
  STRING.encode(state, key)
  return state.buffer.subarray(start, state.start)
}

core.userDataEnd = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_USER_DATA + 1)
  return state.buffer.subarray(start, state.start)
}

core.local = function (ptr, key) {
  if (key.byteLength > 2048) {
    throw new Error('local keys has an upper limit of 2048 bytes atm')
  }

  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_LOCAL)

  state.buffer.set(key, state.start)
  state.start += key.byteLength
  return state.buffer.subarray(start, state.start)
}

core.localEnd = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_LOCAL + 1)
  return state.buffer.subarray(start, state.start)
}

core.blockIndex = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  UINT.decode(state) // ptr
  UINT.decode(state) // type
  return UINT.decode(state)
}

core.bitfieldIndexAndType = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  UINT.decode(state) // ptr
  UINT.decode(state) // type
  return [UINT.decode(state), UINT.decode(state)]
}

core.userDataKey = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  UINT.decode(state) // ptr
  UINT.decode(state) // type
  return STRING.decode(state)
}

core.localKey = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  UINT.decode(state) // ptr
  UINT.decode(state) // type
  return state.buffer.subarray(state.start, state.end)
}

module.exports = { store, core }

function alloc () {
  if (slab.buffer.byteLength - slab.start < 4096) {
    slab.buffer = b4a.allocUnsafe(slab.buffer.byteLength)
    slab.start = 0
  }
  return slab
}
