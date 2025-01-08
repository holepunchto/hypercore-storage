const { UINT, STRING } = require('index-encoder')
const c = require('compact-encoding')
const b4a = require('b4a')

const TL_HEAD = 0
const TL_CORE_BY_DKEY = 1
const TL_CORE = 2
const TL_DATA = 3

const TL_END = TL_DATA

const CORE_AUTH = 0
const CORE_BATCHES = 1

const DATA_HEAD = 0
const DATA_DEPENDENCIES = 1
const DATA_BLOCK = 2
const DATA_TREE = 3
const DATA_BITFIELD = 4
const DATA_USER_DATA = 5

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

store.discoveryKey = function (buffer) {
  const state = { buffer, start: 0, end: buffer.byteLength }
  UINT.decode(state) // ns
  return c.fixed32.decode(state)
}

core.auth = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE)
  UINT.encode(state, ptr)
  UINT.encode(state, CORE_AUTH)
  return state.buffer.subarray(start, state.start)
}

core.batches = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_CORE)
  UINT.encode(state, ptr)
  UINT.encode(state, CORE_BATCHES)
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

core.dependencies = function (ptr) {
  const state = alloc()
  const start = state.start
  UINT.encode(state, TL_DATA)
  UINT.encode(state, ptr)
  UINT.encode(state, DATA_DEPENDENCIES)
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

module.exports = { store, core }

function alloc () {
  if (slab.buffer.byteLength - slab.start < 4096) {
    slab.buffer = b4a.allocUnsafe(slab.buffer.byteLength)
    slab.start = 0
  }
  return slab
}
