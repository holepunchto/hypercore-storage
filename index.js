const RocksDB = require('rocksdb-native')
const rrp = require('resolve-reject-promise')
const ScopeLock = require('scope-lock')
const View = require('./lib/view.js')

const {
  CorestoreRX,
  CorestoreTX,
  CoreTX,
  CoreRX
} = require('./lib/tx.js')

const {
  createCoreStream,
  createAliasStream,
  createBlockStream,
  createBitfieldStream,
  createUserDataStream
} = require('./lib/streams.js')

const EMPTY = new View()

class Atom {
  constructor (db) {
    this.db = db
    this.view = new View()
    this.flushes = []
  }

  onflush (fn) {
    this.flushes.push(fn)
  }

  async flush () {
    await View.flush(this.view.changes, this.db)
    this.view.reset()
    while (this.flushes.length) this.flushes.pop()()
  }
}

class HypercoreStorage {
  constructor (store, db, core, view, atomic) {
    this.store = store
    this.db = db
    this.core = core
    this.view = view
    this.atomic = atomic

    this.view.readStart()
    store.opened++
  }

  get dependencies () {
    return this.core.dependencies
  }

  getDependencyLength () {
    return this.core.dependencies.length
      ? this.core.dependencies[this.core.dependencies.length - 1].length
      : -1
  }

  getDependency (length) {
    for (let i = this.core.dependencies.length - 1; i >= 0; i--) {
      const dep = this.core.dependencies[i]
      if (dep.length < length) return dep
    }

    return null
  }

  // TODO: this might have to be async if the dependents have changed, but prop ok for now
  updateDependencyLength (length) {
    const deps = this.core.dependencies

    for (let i = deps.length - 1; i >= 0; i--) {
      if (deps[i].length >= length) continue
      deps[i].length = length
      this.core.dependencies = deps.slice(0, i + 1)
      return
    }

    throw new Error('Dependency not found')
  }

  snapshot () {
    return new HypercoreStorage(this.store, this.db.snapshot(), this.core, this.view.snapshot(), this.atomic)
  }

  atomize (atom) {
    return new HypercoreStorage(this.store, this.db.session(), this.core, atom.view, true)
  }

  atom () {
    return this.store.atom()
  }

  createBlockStream (start, end, reverse) {
    return createBlockStream(this.core, this.db, this.view, start, end, !!reverse)
  }

  createBitfieldStream (start, end) {
    return createBitfieldStream(this.core, this.db, this.view, start, end)
  }

  createUserDataStream (start, end = null) {
    return createUserDataStream(this.core, this.db, this.view, start, end)
  }

  async resumeBatch (name) {
    const rx = this.read()
    const existingBatchesPromise = rx.getBatches()

    rx.tryFlush()
    const existingBatches = await existingBatchesPromise

    const batches = existingBatches || []
    const batch = getBatch(batches, name, false)

    if (batch === null) return null

    const core = {
      corePointer: this.core.corePointer,
      dataPointer: batch.dataPointer,
      dependencies: []
    }

    const batchRx = new CoreRX(core, this.db, this.view)

    const dependencyPromise = batchRx.getDependency()
    batchRx.tryFlush()

    const dependency = await dependencyPromise
    if (dependency) core.dependencies = this._addDependency(dependency)

    return new HypercoreStorage(this.store, this.db.session(), core, this.atomic ? this.view : new View(), this.atomic)
  }

  async createBatch (name, head) {
    const rx = this.read()

    const existingBatchesPromise = rx.getBatches()
    const existingHeadPromise = rx.getHead()

    rx.tryFlush()

    const [existingBatches, existingHead] = await Promise.all([existingBatchesPromise, existingHeadPromise])
    if (head === null) head = existingHead

    if (existingHead !== null && head.length > existingHead.length) {
      throw new Error('Invalid head passed, ahead of core')
    }

    const batches = existingBatches || []
    const batch = getBatch(batches, name, true)

    batch.dataPointer = await this.store._allocData()

    const tx = this.write()

    tx.setBatches(batches)

    const length = head === null ? 0 : head.length
    const core = {
      corePointer: this.core.corePointer,
      dataPointer: batch.dataPointer,
      dependencies: this._addDependency({ dataPointer: this.core.dataPointer, length })
    }

    const batchTx = new CoreTX(core, this.db, tx.view, tx.changes)

    if (length > 0) batchTx.setHead(head)
    batchTx.setDependency(core.dependencies[core.dependencies.length - 1])

    await tx.flush()

    return new HypercoreStorage(this.store, this.db.session(), core, this.atomic ? this.view : new View(), this.atomic)
  }

  _addDependency (dep) {
    const deps = []

    for (let i = 0; i < this.core.dependencies.length; i++) {
      const d = this.core.dependencies[i]

      if (d.length > dep.length) {
        deps.push({ dataPointer: d.dataPointer, length: dep.length })
        return deps
      }

      deps.push(d)
    }

    deps.push(dep)
    return deps
  }

  read () {
    return new CoreRX(this.core, this.db, this.view)
  }

  write () {
    return new CoreTX(this.core, this.db, this.atomic ? this.view : null, [])
  }

  close () {
    if (this.view !== null) {
      this.store.opened--
      this.view.readStop()
      this.view = null
    }

    return this.db.close()
  }
}

class CorestoreStorage {
  constructor (db) {
    this.db = typeof db === 'string' ? new RocksDB(db) : db
    this.opened = 0
    this.tx = null
    this.enters = 0
    this.lock = new ScopeLock()
    this.flushing = null
  }

  static isCoreStorage (db) {
    return isCorestoreStorage(db)
  }

  static from (db) {
    if (isCorestoreStorage(db)) return db
    return new this(db)
  }

  async _flush () {
    while (this.enters > 0) {
      await this.lock.lock()
      await this.lock.unlock()
    }
  }

  async _enter () {
    this.enters++
    await this.lock.lock()
    if (this.tx === null) this.tx = new CorestoreTX(this.db, new View())
    return this.tx
  }

  async _exit () {
    this.enters--
    this.tx.apply()

    if (this.flushing === null) this.flushing = rrp()
    const flushed = this.flushing.promise

    if (this.enters === 0 || this.tx.view.size() > 128) {
      try {
        await View.flush(this.tx.view.changes, this.db)
        this.flushing.resolve()
      } catch (err) {
        this.flushing.reject(err)
      } finally {
        this.flushing = null
        this.tx = null
      }
    }

    this.lock.unlock()
    return flushed
  }

  // when used with core catches this isnt transactional for simplicity, HOWEVER, its just a number
  // so worth the tradeoff
  async _allocData () {
    let dataPointer = 0

    const tx = await this._enter()

    try {
      const rx = new CorestoreRX(this.db, tx.view)

      const headPromise = rx.getHead()
      rx.tryFlush()

      let head = await headPromise
      if (head === null) head = initStoreHead()

      dataPointer = head.allocated.datas++
      tx.setHead(head)
    } finally {
      await this._exit()
    }

    return dataPointer
  }

  atom () {
    return new Atom(this.db)
  }

  async close () {
    if (this.db.closed) return
    await this._flush()
    await this.db.close()
  }

  async clear () {
    const tx = await this._enter()
    tx.clear()
    await this._exit()
  }

  createCoreStream () {
    return createCoreStream(this.db, EMPTY)
  }

  createAliasStream (namespace) {
    return createAliasStream(this.db, EMPTY, namespace)
  }

  getAlias (alias) {
    const rx = new CorestoreRX(this.db, EMPTY)
    const discoveryKeyPromise = rx.getCoreByAlias(alias)
    rx.tryFlush()
    return discoveryKeyPromise
  }

  async getSeed () {
    const rx = new CorestoreRX(this.db, EMPTY)
    const headPromise = rx.getHead()

    rx.tryFlush()

    const head = await headPromise
    return head === null ? null : head.seed
  }

  async setSeed (seed) {
    const tx = await this._enter()
    try {
      const rx = new CorestoreRX(this.db, tx.view)
      const headPromise = rx.getHead()

      rx.tryFlush()

      const head = (await headPromise) || initStoreHead(null)

      head.seed = seed
      tx.setHead(head)
    } finally {
      await this._exit()
    }
  }

  async getDefaultKey () {
    const rx = new CorestoreRX(this.db, EMPTY)
    const headPromise = rx.getHead()

    rx.tryFlush()

    const head = await headPromise
    return head === null ? null : head.defaultKey
  }

  async setDefaultKey (defaultKey) {
    const tx = await this._enter()
    try {
      const rx = new CorestoreRX(this.db, tx.view)
      const headPromise = rx.getHead()

      rx.tryFlush()

      const head = (await headPromise) || initStoreHead(null)

      head.defaultKey = defaultKey
      tx.setHead(head)
    } finally {
      await this._exit()
    }
  }

  async has (discoveryKey) {
    const rx = new CorestoreRX(this.db, EMPTY)
    const promise = rx.getCore(discoveryKey)

    rx.tryFlush()

    return (await promise) !== null
  }

  async resume (discoveryKey) {
    if (!discoveryKey) {
      discoveryKey = await this.getDefaultKey()
      if (!discoveryKey) return null
    }

    const rx = new CorestoreRX(this.db, EMPTY)
    const corePromise = rx.getCore(discoveryKey)

    rx.tryFlush()
    const core = await corePromise

    if (core === null) return null
    return this._resumeFromPointers(EMPTY, core)
  }

  async _resumeFromPointers (view, { corePointer, dataPointer }) {
    const core = { corePointer, dataPointer, dependencies: [] }

    while (true) {
      const rx = new CoreRX({ dataPointer, corePointer: 0, dependencies: [] }, this.db, view)
      const dependencyPromise = rx.getDependency()
      rx.tryFlush()
      const dependency = await dependencyPromise
      if (!dependency) break
      core.dependencies.push(dependency)
      dataPointer = dependency.dataPointer
    }

    return new HypercoreStorage(this, this.db.session(), core, EMPTY, false)
  }

  // not allowed to throw validation errors as its a shared tx!
  async _create (tx, { key, manifest, keyPair, encryptionKey, discoveryKey, alias, userData }) {
    const rx = new CorestoreRX(this.db, tx.view)

    const corePromise = rx.getCore(discoveryKey)
    const headPromise = rx.getHead()

    rx.tryFlush()

    let [core, head] = await Promise.all([corePromise, headPromise])
    if (core) return this._resumeFromPointers(tx.view, core)

    if (head === null) head = initStoreHead(discoveryKey)

    const corePointer = head.allocated.cores++
    const dataPointer = head.allocated.datas++

    core = { corePointer, dataPointer, alias }

    tx.setHead(head)
    tx.putCore(discoveryKey, core)
    if (alias) tx.putCoreByAlias(alias, discoveryKey)

    const ptr = { corePointer, dataPointer, dependencies: [] }
    const ctx = new CoreTX(ptr, this.db, tx.view, tx.changes)

    ctx.setAuth({
      key,
      discoveryKey,
      manifest,
      keyPair,
      encryptionKey
    })

    if (userData) {
      for (const { key, value } of userData) {
        ctx.putUserData(key, value)
      }
    }

    return new HypercoreStorage(this, this.db.session(), ptr, EMPTY, false)
  }

  async create (data) {
    const tx = await this._enter()

    try {
      return await this._create(tx, data)
    } finally {
      await this._exit()
    }
  }
}

module.exports = CorestoreStorage

function initStoreHead (defaultKey) {
  return {
    version: 0,
    allocated: {
      datas: 0,
      cores: 0
    },
    seed: null,
    defaultKey
  }
}

function getBatch (batches, name, alloc) {
  for (let i = 0; i < batches.length; i++) {
    if (batches[i].name === name) return batches[i]
  }

  if (!alloc) return null

  const result = { name, dataPointer: 0 }
  batches.push(result)
  return result
}

function isCorestoreStorage (s) {
  return typeof s === 'object' && !!s && typeof s.setDefaultKey === 'function'
}
