const ScopeLock = require('scope-lock')
const { CorestoreRX, CorestoreTX, CoreTX, CoreRX } = require('./lib/tx.js')
const Updates = require('./lib/updates.js')
const { createDiscoveryKeyStream } = require('./lib/streams.js')
const rrp = require('resolve-reject-promise')

const EMPTY = new Updates()

class HypercoreStorage {
  constructor (store, db, core) {
    this.store = store
    this.db = db
    this.core = core
  }

  async resumeBatch (tx, name) {
    const rx = this.createReadBatch(tx)
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

    const storage = new HypercoreStorage(this.store, this.db, core)
    const batchRx = storage.createReadBatch(tx)

    const dependenciesPromise = batchRx.getDependencies()
    batchRx.tryFlush()

    const dependencies = await dependenciesPromise
    if (dependencies) core.dependencies = dependencies

    return storage
  }

  async createBatch (tx, name, head) {
    const rx = this.createReadBatch(tx)

    const existingBatchesPromise = rx.getBatches()
    const existingHeadPromise = rx.getHead()

    rx.tryFlush()

    const [existingBatches, existingHead] = await Promise.all([existingBatchesPromise, existingHeadPromise])
    if (head === null) head = existingHead

    const batches = existingBatches || []
    const batch = getBatch(batches, name, true)

    batch.dataPointer = await this.store._allocData()

    tx.setBatches(batches)

    const length = head === null ? 0 : head.length
    const core = {
      corePointer: this.core.corePointer,
      dataPointer: batch.dataPointer,
      dependencies: []
    }

    if (length > 0) core.dependencies.push({ dataPointer: this.core.dataPointer, length })

    const storage = new HypercoreStorage(this.store, this.db, core)
    const batchTx = storage.createWriteBatch(tx)

    if (length > 0) batchTx.setHead(head)
    batchTx.setDependencies(core.dependencies)

    return storage
  }

  createReadBatch (tx) {
    const updates = tx ? tx.updates : EMPTY
    return new CoreRX(this.core, this.db, updates)
  }

  createWriteBatch (tx) {
    const updates = tx ? tx.updates : new Updates()
    return new CoreTX(this.core, this.db, updates)
  }

  flush (tx) {
    return tx.updates.flush(this.db)
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
    if (this.tx === null) this.tx = new CorestoreTX(this.db, new Updates())
    return this.tx
  }

  async _exit () {
    this.enters--

    if (this.flushing === null) this.flushing = rrp()
    const flushed = this.flushing.promise

    if (this.enters === 0 || this.tx.updates.size() > 128) {
      try {
        await this.tx.updates.flush(this.db)
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
      const rx = new CorestoreRX(this.db, tx.updates)

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

  list () {
    return createDiscoveryKeyStream(this.db, EMPTY)
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

  async _resumeFromPointers (updates, { corePointer, dataPointer }) {
    const rx = new CorestoreRX(this.db, updates)
    const dependenciesPromise = rx.getDependencies()

    rx.tryFlush()
    const dependencies = await dependenciesPromise

    const result = {
      corePointer,
      dataPointer,
      dependencies: dependencies || []
    }

    return new HypercoreStorage(this, this.db, result)
  }

  // not allowed to throw validation errors as its a shared tx!
  async _create (tx, { key, manifest, keyPair, encryptionKey, discoveryKey, userData }) {
    const rx = new CorestoreRX(this.db, tx.updates)

    const corePromise = rx.getCore(discoveryKey)
    const headPromise = rx.getHead()

    rx.tryFlush()

    let [core, head] = await Promise.all([corePromise, headPromise])
    if (core) return this._resumeFromPointers(tx.updates, core)

    if (head === null) head = initStoreHead()

    const corePointer = head.allocated.cores++
    const dataPointer = head.allocated.datas++

    core = { corePointer, dataPointer }

    tx.putCore(discoveryKey, core)
    tx.setHead(head)

    const ptr = { corePointer, dataPointer, dependencies: [] }
    const ctx = new CoreTX(ptr, this.db, tx.updates)

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

    return new HypercoreStorage(this, this.db, ptr)
  }

  async create ({ key, manifest, keyPair, encryptionKey, discoveryKey, userData }) {
    const tx = await this._enter()

    try {
      return await this._create(tx, { key, manifest, keyPair, encryptionKey, discoveryKey, userData })
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
