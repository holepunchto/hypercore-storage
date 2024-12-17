const RocksDB = require('rocksdb-native')
const c = require('compact-encoding')
const { UINT } = require('index-encoder')
const RW = require('read-write-mutexify')
const b4a = require('b4a')
const flat = require('flat-tree')
const rrp = require('resolve-reject-promise')
const queueTick = require('queue-tick')
const assert = require('nanoassert')

const m = require('./lib/messages')
const DependencyStream = require('./lib/dependency-stream')
const MemoryOverlay = require('./lib/memory-overlay')

const INF = b4a.from([0xff])

// <TL_INFO> = { version, free, total }
// <TL_LOCAL_SEED> = seed
// <TL_CORE_INFO><discovery-key-32-bytes> = { version, owner, core, data }

// <core><CORE_MANIFEST>        = { key, manifest? }
// <core><CORE_LOCAL_SEED>      = seed
// <core><CORE_ENCRYPTION_KEY>  = encryptionKey // should come later, not important initially
// <core><CORE_HEAD><data>      = { fork, length, byteLength, signature }
// <core><CORE_BATCHES><name>   = <data>

// <data><CORE_INFO>            = { version }
// <data><CORE_UPDATES>         = { contiguousLength, blocks }
// <data><CORE_DEPENDENCY       = { data, length, roots }
// <data><CORE_HINTS>           = { reorg } // should come later, not important initially
// <data><CORE_TREE><index>     = { index, size, hash }
// <data><CORE_BITFIELD><index> = <4kb buffer>
// <data><CORE_BLOCKS><index>   = <buffer>
// <data><CORE_USER_DATA><key>  = <value>

// top level prefixes
const TL = {
  STORAGE_INFO: 0,
  LOCAL_SEED: 1,
  DKEYS: 2,
  CORE: 3,
  DATA: 4,
  DEFAULT_KEY: 5
}

// core prefixes
const CORE = {
  MANIFEST: 0,
  LOCAL_SEED: 1,
  ENCRYPTION_KEY: 2,
  HEAD: 3,
  BATCHES: 4
}

// data prefixes
const DATA = {
  INFO: 0,
  UPDATES: 1,
  DEPENDENCY: 2,
  HINTS: 3,
  TREE: 4,
  BITFIELD: 5,
  BLOCK: 6,
  USER_DATA: 7
}

const SLAB = {
  start: 0,
  end: 65536,
  buffer: b4a.allocUnsafe(65536)
}

// PREFIX + BATCH + TYPE + INDEX

class WriteBatch {
  constructor (storage, write, atom) {
    this.storage = storage
    this.write = write
    this.atom = atom
  }

  setCoreHead (head) {
    this.write.tryPut(encodeCoreIndex(this.storage.corePointer, CORE.HEAD, this.storage.dataPointer), c.encode(m.CoreHead, head))
  }

  setCoreAuth ({ key, manifest }) {
    this.write.tryPut(encodeCoreIndex(this.storage.corePointer, CORE.MANIFEST), c.encode(m.CoreAuth, { key, manifest }))
  }

  setBatchPointer (name, pointer) {
    this.write.tryPut(encodeBatch(this.storage.corePointer, CORE.BATCHES, name), encode(m.DataPointer, pointer))
  }

  setDataDependency ({ data, length }) {
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.DEPENDENCY), encode(m.DataDependency, { data, length }))
  }

  setLocalKeyPair (keyPair) {
    this.write.tryPut(encodeCoreIndex(this.storage.corePointer, CORE.LOCAL_SEED), encode(m.KeyPair, keyPair))
  }

  setEncryptionKey (encryptionKey) {
    this.write.tryPut(encodeCoreIndex(this.storage.corePointer, CORE.ENCRYPTION_KEY), encryptionKey)
  }

  setDataInfo (info) {
    if (info.version !== 0) throw new Error('Version > 0 is not supported')
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.INFO), encode(m.DataInfo, info))
  }

  setUserData (key, value) {
    this.write.tryPut(encodeUserDataIndex(this.storage.dataPointer, DATA.USER_DATA, key), value)
  }

  putBlock (index, data) {
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index), data)
  }

  deleteBlock (index) {
    this.write.tryDelete(encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index))
  }

  deleteBlockRange (start, end) {
    return this._deleteRange(DATA.BLOCK, start, end)
  }

  putTreeNode (node) {
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.TREE, node.index), encode(m.TreeNode, node))
  }

  deleteTreeNode (index) {
    this.write.tryDelete(encodeDataIndex(this.storage.dataPointer, DATA.TREE, index))
  }

  deleteTreeNodeRange (start, end) {
    return this._deleteRange(DATA.TREE, start, end)
  }

  putBitfieldPage (index, page) {
    this.write.tryPut(encodeDataIndex(this.storage.dataPointer, DATA.BITFIELD, index), page)
  }

  deleteBitfieldPage (index) {
    this.write.tryDelete(encodeDataIndex(this.storage.dataPointer, DATA.BITFIELD, index))
  }

  deleteBitfieldPageRange (start, end) {
    return this._deleteRange(DATA.BITFIELD, start, end)
  }

  _deleteRange (type, start, end) {
    const s = encodeDataIndex(this.storage.dataPointer, type, start)
    const e = encodeDataIndex(this.storage.dataPointer, type, end === -1 ? Infinity : end)

    return this.write.deleteRange(s, e)
  }

  destroy () {
    if (this.atom) this.atom.destroy()
    else this.write.destroy()
  }

  flush () {
    if (this.atom) return this.atom.flush()
    return this.write.flush()
  }
}

class ReadBatch {
  constructor (storage, read) {
    this.storage = storage
    this.read = read
  }

  async getCoreHead () {
    return this._get(encodeCoreIndex(this.storage.corePointer, CORE.HEAD, this.storage.dataPointer), m.CoreHead)
  }

  async getCoreAuth () {
    return this._get(encodeCoreIndex(this.storage.corePointer, CORE.MANIFEST), m.CoreAuth)
  }

  async getLocalKeyPair () {
    return this._get(encodeCoreIndex(this.storage.corePointer, CORE.LOCAL_SEED), m.KeyPair)
  }

  async getEncryptionKey () {
    return this._get(encodeCoreIndex(this.storage.corePointer, CORE.ENCRYPTION_KEY), null)
  }

  getDataDependency () {
    return this._get(encodeDataIndex(this.storage.dataPointer, DATA.DEPENDENCY), m.DataDependency)
  }

  getDataInfo (info) {
    return this._get(encodeDataIndex(this.storage.dataPointer, DATA.INFO), m.DataInfo)
  }

  getUserData (key) {
    return this._get(encodeUserDataIndex(this.storage.dataPointer, DATA.USER_DATA, key), null)
  }

  async hasBlock (index) {
    return this._has(encodeDataIndex(this.storage.dataPointer, DATA.BLOCK, index))
  }

  async getBlock (index, error) {
    const dependency = findBlockDependency(this.storage.dependencies, index)
    const dataPointer = dependency !== null ? dependency : this.storage.dataPointer

    const key = encodeDataIndex(dataPointer, DATA.BLOCK, index)
    const block = await this._get(key, null)

    if (block === null && error === true) {
      throw new Error('Node not found: ' + index)
    }

    return block
  }

  async hasTreeNode (index) {
    return this._has(encodeDataIndex(this.storage.dataPointer, DATA.TREE, index))
  }

  async getTreeNode (index, error) {
    const dependency = findTreeDependency(this.storage.dependencies, index)
    const dataPointer = dependency !== null ? dependency : this.storage.dataPointer

    const key = encodeDataIndex(dataPointer, DATA.TREE, index)
    const node = await this._get(key, m.TreeNode)

    if (node === null && error === true) {
      throw new Error('Node not found: ' + index)
    }

    return node
  }

  async getBitfieldPage (index) {
    const key = encodeDataIndex(this.storage.dataPointer, DATA.BITFIELD, index)
    return this._get(key, null)
  }

  async _has (key) {
    return (await this.read.get(key)) !== null
  }

  async _get (key, enc) {
    const buffer = await this.read.get(key)
    if (buffer === null) return null

    if (enc) return c.decode(enc, buffer)

    return buffer
  }

  destroy () {
    this.read.destroy()
  }

  flush () {
    return this.read.flush()
  }

  tryFlush () {
    this.read.tryFlush()
  }
}

class Atomizer {
  constructor (db) {
    this.db = db
    this.batch = null
    this.refs = 0
    this.destroyed = false
    this.flushing = null
    this.resolve = null
    this.reject = null

    this._executing = null
    this._waiting = []
    this._queue = []
    this._enqueue = (resolve, reject) => this._queue.push({ resolve, reject })
  }

  enter () {
    this.refs++
  }

  exit () {
    if (--this.refs === 0) this._commit()
  }

  acquire (mutex) {
    this._lock(mutex)
    return new Promise(this._enqueue)
  }

  async _lock (mutex) {
    for (const lock of this._waiting) {
      if (lock.mutex === mutex) return
    }

    this._waiting.push({ mutex, promise: mutex.lock() })

    if (this._executing === null) this._executing = this._execute()
  }

  async _execute () {
    this.enter()
    for (const { promise } of this._waiting) await promise

    const queue = this._queue

    this._waiting = []
    this._queue = []
    this._executing = null

    for (const { resolve, reject } of queue) {
      if (this.destroyed) reject(new Error('Atomizer destroyed'))
      else resolve()
    }

    this._ensureTick() // allow caller to enter
    this.exit()
  }

  createBatch () {
    if (this.refs === 0) this._ensureTick()
    this.enter()
    if (this.batch === null) this.batch = this.db.write()
    return this.batch
  }

  _ensureTick () {
    this.enter()
    queueTick(() => this.exit())
  }

  async _commit () {
    if (this.batch === null) return

    const batch = this.batch
    const resolve = this.resolve
    const reject = this.reject

    this.batch = null
    this.flushing = null
    this.resolve = this.reject = null

    if (this.destroyed) {
      this.destroyed = false
      batch.destroy()
      if (reject !== null) reject(new Error('Atomic batch destroyed'))
      return
    }

    try {
      await batch.flush()
    } catch (err) {
      reject(err)
      return
    }

    resolve()
  }

  _createFlushing () {
    if (this.flushing !== null) return this.flushing

    const { resolve, reject, promise } = rrp()

    this.flushing = promise
    this.resolve = resolve
    this.reject = reject

    return this.flushing
  }

  destroy () {
    this.destroyed = true
    this.exit()
  }

  flush () {
    const flushing = this._createFlushing()
    this.exit()
    return flushing
  }
}

module.exports = class CoreStorage {
  constructor (dir) {
    this.db = new RocksDB(dir)
    this.mutex = new RW()
  }

  // just a helper to make tests easier
  static async clear (dir) {
    const s = new this(dir)
    await s.clear()
    return s
  }

  async setLocalSeed (seed, overwrite) {
    if (!overwrite) {
      const existing = await getLocalSeed(this.db)
      if (existing) return b4a.equals(existing, seed)
    }

    await this.mutex.write.lock()

    try {
      const b = this.db.write()
      b.tryPut(b4a.from([TL.LOCAL_SEED]), seed)
      await b.flush()

      return true
    } finally {
      this.mutex.write.unlock()
    }
  }

  getLocalSeed () {
    return getLocalSeed(this.db)
  }

  info () {
    return getStorageInfo(this.db)
  }

  list () {
    const s = this.db.iterator({
      gt: b4a.from([TL.DKEYS]),
      lt: b4a.from([TL.DKEYS + 1])
    })

    s._readableState.map = mapOnlyDiscoveryKey
    return s
  }

  async idle () {
    if (this.isIdle()) return

    do {
      await new Promise(setImmediate)
      await this.db.idle()
      await new Promise(setImmediate)
    } while (!this.isIdle())
  }

  isIdle () {
    return this.db.isIdle()
  }

  ready () {
    return this.db.ready()
  }

  close () {
    return this.db.close()
  }

  atomizer () {
    return new Atomizer(this.db)
  }

  async clear () {
    const b = this.db.write()
    b.tryDeleteRange(b4a.from([TL.STORAGE_INFO]), INF)
    await b.flush()
  }

  async has (discoveryKey) {
    return !!(await this.db.get(encodeDiscoveryKey(discoveryKey)))
  }

  async resume (discoveryKey) {
    if (!discoveryKey) {
      discoveryKey = await getDefaultKey(this.db)
      if (!discoveryKey) return null
    }

    const val = await this.db.get(encodeDiscoveryKey(discoveryKey))
    if (val === null) return null

    const { core, data } = c.decode(m.CorePointer, val)

    return new HypercoreStorage(this, discoveryKey, core, data, null)
  }

  async create ({ key, manifest, keyPair, encryptionKey, discoveryKey, userData }) {
    await this.mutex.write.lock()

    try {
      const existing = await this.resume(discoveryKey)

      if (existing) {
        // todo: verify key/manifest etc.
        return existing
      }

      if (!key) throw new Error('No key was provided')

      let info = await getStorageInfo(this.db)

      const write = this.db.write()

      if (!info) {
        write.tryPut(b4a.from([TL.DEFAULT_KEY]), discoveryKey)
        info = { free: 0, total: 0 }
      }

      const core = info.total++
      const data = info.free++

      write.tryPut(encodeDiscoveryKey(discoveryKey), encode(m.CorePointer, { core, data }))
      write.tryPut(b4a.from([TL.STORAGE_INFO]), encode(m.StorageInfo, info))

      const storage = new HypercoreStorage(this, discoveryKey, core, data, null)
      const batch = new WriteBatch(storage, write, null)

      initialiseCoreInfo(batch, { key, manifest, keyPair, encryptionKey })
      initialiseCoreData(batch, { userData })

      await batch.flush()
      return storage
    } finally {
      this.mutex.write.unlock()
    }
  }
}

class HypercoreStorage {
  constructor (root, discoveryKey, core, data, snapshot) {
    this.root = root
    this.db = root.db
    this.dbSnapshot = snapshot
    this.mutex = root.mutex

    this.discoveryKey = discoveryKey

    this.dependencies = []

    // pointers
    this.corePointer = core
    this.dataPointer = data

    this.destroyed = false
  }

  get snapshotted () {
    return this.dbSnapshot !== null
  }

  atomizer () {
    return this.root.atomizer()
  }

  dependencyLength () {
    return this.dependencies.length
      ? this.dependencies[this.dependencies.length - 1].length
      : -1
  }

  async openBatch (name) {
    const existing = await this.db.get(encodeBatch(this.corePointer, CORE.BATCHES, name))
    if (!existing) return null

    const storage = new HypercoreStorage(this.root, this.discoveryKey, this.corePointer, this.dataPointer, this.dbSnapshot)
    const dataPointer = c.decode(m.DataPointer, existing)

    storage.dataPointer = dataPointer
    storage.dependencies = await addDependencies(this.db, storage.dataPointer, -1)

    return storage
  }

  async registerBatch (name, head) {
    await this.mutex.write.lock()

    const storage = new HypercoreStorage(this.root, this.discoveryKey, this.corePointer, this.dataPointer, null)

    try {
      const info = await getStorageInfo(this.db)
      const write = this.db.write()

      storage.dataPointer = info.free++

      write.tryPut(b4a.from([TL.STORAGE_INFO]), encode(m.StorageInfo, info))

      const batch = new WriteBatch(storage, write, null)

      initialiseCoreData(batch)

      batch.setDataDependency({ data: this.dataPointer, length: head.length })
      batch.setBatchPointer(name, storage.dataPointer)
      if (head.rootHash) batch.setCoreHead(head) // if no root hash its the empty core - no head yet

      await write.flush()

      storage.dependencies = await addDependencies(this.db, storage.dataPointer, head.length)
      return storage
    } finally {
      this.mutex.write.unlock()
    }
  }

  async registerOverlay (head) {
    const storage = new MemoryOverlay(this)
    const batch = storage.createWriteBatch()

    batch.setDataDependency({ data: this.dataPointer, length: head.length })
    if (head.rootHash) batch.setCoreHead(head) // if no root hash its the empty core - no head yet

    await batch.flush()

    return storage
  }

  createMemoryOverlay () {
    return new MemoryOverlay(this)
  }

  snapshot () {
    assert(this.destroyed === false)
    const s = new HypercoreStorage(this.root, this.discoveryKey, this.corePointer, this.dataPointer, this.db.snapshot())

    for (const { data, length } of this.dependencies) s.dependencies.push({ data, length })

    return s
  }

  findDependency (length) {
    for (let i = this.dependencies.length - 1; i >= 0; i--) {
      const dep = this.dependencies[i]
      if (dep.length < length) return dep
    }

    return null
  }

  updateDependencies (length) {
    const deps = this.dependencies

    for (let i = deps.length - 1; i >= 0; i--) {
      if (deps[i].length < length) {
        deps[i].length = length
        this.dependencies = deps.slice(0, i + 1)
        return
      }
    }

    throw new Error('Dependency not found')
  }

  createReadBatch (opts) {
    assert(this.destroyed === false)

    const snapshot = this.dbSnapshot
    return new ReadBatch(this, this.db.read({ snapshot }))
  }

  createWriteBatch (atomizer) {
    assert(this.destroyed === false)

    if (atomizer) return new WriteBatch(this, atomizer.createBatch(), atomizer)

    return new WriteBatch(this, this.db.write(), null)
  }

  createBlockStream (opts = {}) {
    assert(this.destroyed === false)
    return createStream(this, createBlockStream, opts)
  }

  createUserDataStream (opts = {}) {
    assert(this.destroyed === false)

    const r = encodeUserDataRange(this.dataPointer, DATA.USER_DATA, this.dbSnapshot, opts)
    const s = this.db.iterator(r)
    s._readableState.map = mapStreamUserData
    return s
  }

  createTreeNodeStream (opts = {}) {
    assert(this.destroyed === false)

    const r = encodeIndexRange(this.dataPointer, DATA.TREE, this.dbSnapshot, opts)
    const s = this.db.iterator(r)
    s._readableState.map = mapStreamTreeNode
    return s
  }

  createBitfieldPageStream (opts = {}) {
    assert(this.destroyed === false)

    const r = encodeIndexRange(this.dataPointer, DATA.BITFIELD, this.dbSnapshot, opts)
    const s = this.db.iterator(r)
    s._readableState.map = mapStreamBitfieldPage
    return s
  }

  async peekLastTreeNode () {
    assert(this.destroyed === false)

    const last = await this.db.peek(encodeIndexRange(this.dataPointer, DATA.TREE, this.dbSnapshot, { reverse: true }))
    if (last === null) return null
    return c.decode(m.TreeNode, last.value)
  }

  async peekLastBitfieldPage () {
    assert(this.destroyed === false)

    const last = await this.db.peek(encodeIndexRange(this.dataPointer, DATA.BITFIELD, this.dbSnapshot, { reverse: true }))
    if (last === null) return null
    return mapStreamBitfieldPage(last)
  }

  destroy () {
    if (this.destroyed) return
    this.destroyed = true

    if (this.dbSnapshot) this.dbSnapshot.destroy()
    this.dbSnapshot = null
  }
}

function createStream (storage, createStreamType, opts) {
  return storage.dependencies.length === 0
    ? createStreamType(storage.db, storage.dbSnapshot, storage.dataPointer, opts)
    : new DependencyStream(storage, createStreamType, opts)
}

function createBlockStream (db, snap, data, opts) {
  const r = encodeIndexRange(data, DATA.BLOCK, snap, opts)
  const s = db.iterator(r)
  s._readableState.map = mapStreamBlock
  return s
}

function mapStreamUserData (data) {
  const state = { start: 0, end: data.key.byteLength, buffer: data.key }

  UINT.decode(state) // TL.DATA
  UINT.decode(state) // pointer
  UINT.decode(state) // DATA.USER_DATA

  const key = c.string.decode(state)

  if (data.value.byteLength === 0) return null

  return { key, value: data.value }
}

function mapStreamTreeNode (data) {
  return c.decode(m.TreeNode, data.value)
}

function mapStreamBitfieldPage (data) {
  const state = { start: 0, end: data.key.byteLength, buffer: data.key }

  UINT.decode(state) // TL.DATA
  UINT.decode(state) // pointer
  UINT.decode(state) // DATA.BITFIELD

  const index = UINT.decode(state)

  return { index, page: data.value }
}

function mapStreamBlock (data) {
  const state = { start: 0, end: data.key.byteLength, buffer: data.key }

  UINT.decode(state) // TL.DATA
  UINT.decode(state) // pointer
  UINT.decode(state) // DATA.BITFIELD

  const index = UINT.decode(state)
  return { index, value: data.value }
}

function mapOnlyDiscoveryKey (data) {
  return data.key.subarray(1)
}

async function getDefaultKey (db) {
  return db.get(b4a.from([TL.DEFAULT_KEY]))
}

async function getLocalSeed (db) {
  return db.get(b4a.from([TL.LOCAL_SEED]))
}

async function getStorageInfo (db) {
  const value = await db.get(b4a.from([TL.STORAGE_INFO]))
  if (value === null) return null
  return c.decode(m.StorageInfo, value)
}

function ensureSlab (size) {
  if (SLAB.buffer.byteLength - SLAB.start < size) {
    SLAB.buffer = b4a.allocUnsafe(SLAB.end)
    SLAB.start = 0
  }

  return SLAB
}

function encodeIndexRange (pointer, type, snapshot, opts) {
  const bounded = { snapshot, gt: null, gte: null, lte: null, lt: null, reverse: !!opts.reverse, limit: toLimit(opts.limit) }

  if (opts.gt || opts.gt === 0) bounded.gt = encodeDataIndex(pointer, type, opts.gt)
  else if (opts.gte) bounded.gte = encodeDataIndex(pointer, type, opts.gte)
  else bounded.gte = encodeDataIndex(pointer, type, 0)

  if (opts.lt || opts.lt === 0) bounded.lt = encodeDataIndex(pointer, type, opts.lt)
  else if (opts.lte) bounded.lte = encodeDataIndex(pointer, type, opts.lte)
  else bounded.lte = encodeDataIndex(pointer, type, Infinity) // infinity

  return bounded
}

function encodeUserDataRange (pointer, type, snapshot, opts) {
  const bounded = { snapshot, gt: null, gte: null, lte: null, lt: null, reverse: !!opts.reverse, limit: toLimit(opts.limit) }

  if (opts.gt || opts.gt === 0) bounded.gt = encodeUserDataIndex(pointer, type, opts.gt)
  else if (opts.gte) bounded.gte = encodeUserDataIndex(pointer, type, opts.gte)
  else bounded.gte = encodeDataIndex(pointer, type, 0)

  if (opts.lt || opts.lt === 0) bounded.lt = encodeUserDataIndex(pointer, type, opts.lt)
  else if (opts.lte) bounded.lte = encodeUserDataIndex(pointer, type, opts.lte)
  else bounded.lte = encodeDataIndex(pointer, type, Infinity)

  return bounded
}

function toLimit (n) {
  return n === 0 ? 0 : (n || Infinity)
}

function encode (encoding, value) {
  const state = ensureSlab(128)
  const start = state.start
  encoding.encode(state, value)

  assert(state.start <= state.end)

  return state.buffer.subarray(start, state.start)
}

function encodeBatch (pointer, type, name) {
  const end = 128 + name.length
  const state = { start: 0, end, buffer: b4a.allocUnsafe(end) }
  const start = state.start
  UINT.encode(state, TL.CORE)
  UINT.encode(state, pointer)
  UINT.encode(state, type)
  c.string.encode(state, name)

  return state.buffer.subarray(start, state.start)
}

function encodeCoreIndex (pointer, type, index) {
  const state = ensureSlab(128)
  const start = state.start
  UINT.encode(state, TL.CORE)
  UINT.encode(state, pointer)
  UINT.encode(state, type)
  if (index !== undefined) UINT.encode(state, index)

  return state.buffer.subarray(start, state.start)
}

function encodeDataIndex (pointer, type, index) {
  const state = ensureSlab(128)
  const start = state.start
  UINT.encode(state, TL.DATA)
  UINT.encode(state, pointer)
  UINT.encode(state, type)
  if (index !== undefined) UINT.encode(state, index)

  return state.buffer.subarray(start, state.start)
}

function encodeUserDataIndex (pointer, type, key) {
  const end = 128 + key.length
  const state = { start: 0, end, buffer: b4a.allocUnsafe(end) }
  const start = state.start
  UINT.encode(state, TL.DATA)
  UINT.encode(state, pointer)
  UINT.encode(state, type)
  c.string.encode(state, key)

  return state.buffer.subarray(start, state.start)
}

function encodeDiscoveryKey (discoveryKey) {
  const state = ensureSlab(128)
  const start = state.start
  UINT.encode(state, TL.DKEYS)
  c.fixed32.encode(state, discoveryKey)
  return state.buffer.subarray(start, state.start)
}

async function addDependencies (db, dataPointer, treeLength) {
  const dependencies = []

  let dep = await db.get(encodeDataIndex(dataPointer, DATA.DEPENDENCY))
  while (dep) {
    const { data, length } = c.decode(m.DataDependency, dep)
    if (treeLength === -1 || length <= treeLength) dependencies.push({ data, length })

    dep = await db.get(encodeDataIndex(data, DATA.DEPENDENCY))
  }

  return dependencies
}

function findBlockDependency (dependencies, index) {
  for (const { data, length } of dependencies) {
    if (index < length) return data
  }
  return null
}

function findTreeDependency (dependencies, index) {
  for (const { data, length } of dependencies) {
    if (flat.rightSpan(index) <= (length - 1) * 2) return data
  }
  return null
}

function initialiseCoreInfo (db, { key, manifest, keyPair, encryptionKey }) {
  db.setCoreAuth({ key, manifest })
  if (keyPair) db.setLocalKeyPair(keyPair)
  if (encryptionKey) db.setEncryptionKey(encryptionKey)
}

function initialiseCoreData (db, { userData } = {}) {
  db.setDataInfo({ version: 0 })
  if (userData) {
    for (const { key, value } of userData) {
      db.setUserData(key, value)
    }
  }
}
