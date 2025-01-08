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
  createDiscoveryKeyStream,
  createAliasStream,
  createBlockStream,
  createBitfieldStream,
  createUserDataStream
} = require('./lib/streams.js')

const EMPTY = new View()

class HypercoreStorage {
  constructor (store, db, core, view) {
    this.store = store
    this.db = db
    this.core = core
    this.view = view
    this.atom = false

    this.view.readStart()
  }

  snapshot () {
    return new HypercoreStorage(this.store, this.db.snapshot(), this.core, this.view.snapshot())
  }

  createBlockStream (start, end) {
    return createBlockStream(this.core, this.db, this.view, start, end)
  }

  createBitfieldStream (start, end) {
    return createBitfieldStream(this.core, this.db, this.view, start, end)
  }

  createUserDataStream (start, end) {
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

    const batchRx = new CoreRX(this.core, this.db, this.view)

    const dependenciesPromise = batchRx.getDependencies()
    batchRx.tryFlush()

    const dependencies = await dependenciesPromise
    if (dependencies) core.dependencies = dependencies

    return new HypercoreStorage(this.store, this.db.session(), core, this.atom ? this.view : new View())
  }

  async createBatch (name, head) {
    const rx = this.read()

    const existingBatchesPromise = rx.getBatches()
    const existingHeadPromise = rx.getHead()

    rx.tryFlush()

    const [existingBatches, existingHead] = await Promise.all([existingBatchesPromise, existingHeadPromise])
    if (head === null) head = existingHead

    const batches = existingBatches || []
    const batch = getBatch(batches, name, true)

    batch.dataPointer = await this.store._allocData()

    const tx = this.write()

    tx.setBatches(batches)

    const length = head === null ? 0 : head.length
    const core = {
      corePointer: this.core.corePointer,
      dataPointer: batch.dataPointer,
      dependencies: []
    }

    if (length > 0) core.dependencies.push({ dataPointer: this.core.dataPointer, length })

    const batchTx = new CoreTX(core, this.db, tx.view, tx.changes)

    if (length > 0) batchTx.setHead(head)
    batchTx.setDependencies(core.dependencies)

    await tx.flush()

    return new HypercoreStorage(this.store, this.db.session(), core, this.atom ? this.view : new View())
  }

  read () {
    return new CoreRX(this.core, this.db, this.view)
  }

  write () {
    return new CoreTX(this.core, this.db, this.atom ? this.view : null, [])
  }

  close () {
    if (this.view !== null) {
      this.view.readStop()
      this.view = null
    }

    return this.db.close()
  }
}

class CorestoreStorage {
  constructor (db) {
    this.db = db
    this.tx = null
    this.enters = 0
    this.lock = new ScopeLock()
    this.flushing = null
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
        await View.flush(this.tx.changes, this.db)
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

  async close () {
    await this._enter()
    await this._exit()
    await this.db.close()
  }

  async clear () {
    const tx = await this._enter()
    tx.clear()
    await this._exit()
  }

  createDiscoveryKeyStream () {
    return createDiscoveryKeyStream(this.db, EMPTY)
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

      const head = (await headPromise) || initStoreHead()

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

      const head = (await headPromise) || initStoreHead()

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
    const rx = new CorestoreRX(this.db, EMPTY)
    const corePromise = rx.getCore(discoveryKey)

    rx.tryFlush()
    const core = await corePromise

    if (core === null) return null
    return this._resumeFromPointers(EMPTY, core)
  }

  async _resumeFromPointers (view, { corePointer, dataPointer }) {
    const rx = new CoreRX({ corePointer, dataPointer, dependencies: [] }, this.db, view)
    const dependenciesPromise = rx.getDependencies()

    rx.tryFlush()
    const dependencies = await dependenciesPromise

    const result = {
      corePointer,
      dataPointer,
      dependencies: dependencies || []
    }

    return new HypercoreStorage(this, this.db.session(), result, EMPTY)
  }

  // not allowed to throw validation errors as its a shared tx!
  async _create (tx, { key, manifest, keyPair, encryptionKey, discoveryKey, alias, userData }) {
    const rx = new CorestoreRX(this.db, tx.view)

    const corePromise = rx.getCore(discoveryKey)
    const headPromise = rx.getHead()

    rx.tryFlush()

    let [core, head] = await Promise.all([corePromise, headPromise])
    if (core) return this._resumeFromPointers(tx.view, core)

    if (head === null) head = initStoreHead()

    const corePointer = head.allocated.cores++
    const dataPointer = head.allocated.datas++

    core = { corePointer, dataPointer, alias }

    tx.setHead(head)
    tx.putCore(discoveryKey, core)
    if (alias) tx.putCoreByAlias(alias, discoveryKey)

    const ptr = { corePointer, dataPointer, dependencies: [] }
    const ctx = new CoreTX(ptr, this.db, tx.view, true, tx.changes)

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

    return new HypercoreStorage(this, this.db.session(), ptr, EMPTY)
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

function initStoreHead () {
  return {
    version: 0,
    allocated: {
      datas: 0,
      cores: 0
    },
    seed: null
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
