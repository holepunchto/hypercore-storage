const ScopeLock = require('scope-lock')
const { CorestoreRX, CorestoreTX, CoreTX } = require('./lib/tx.js')
const Updates = require('./lib/updates.js')
const { createDiscoveryKeyStream } = require('./lib/streams.js')

class CorestoreStorage {
  constructor (db) {
    this.db = db
    this.updates = new Updates()
    this.tx = null
    this.enters = 0
    this.lock = new ScopeLock()
  }

  async _enter () {
    this.enters++
    await this.lock.lock()
    if (this.tx === null) this.tx = new CorestoreTX(this.db, new Updates())
    return this.tx
  }

  async _exit () {
    this.enters--

    const flushed = this.tx.flushed()

    if (this.enters === 0 || this.tx.updates.size() > 128) {
      await this.tx.flush()
      this.tx = null
    }

    this.lock.unlock()
    return flushed
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
    return createDiscoveryKeyStream(this.db, this.updates)
  }

  async has (discoveryKey) {
    const rx = new CorestoreRX(this.db, this.updates)
    const promise = rx.getCore(discoveryKey)

    rx.tryFlush()

    return (await promise) !== null
  }

  async resume (discoveryKey) {
    const rx = new CorestoreRX(this.db, this.updates)
    const promise = rx.getCore(discoveryKey)

    rx.tryFlush()

    return await promise
  }

  // not allowed to throw validation errors as its a shared tx!
  async _create (tx, { key, manifest, keyPair, encryptionKey, discoveryKey, userData }) {
    const rx = new CorestoreRX(this.db, tx.updates)

    const headPromise = rx.getHead()
    const corePromise = rx.getCore(discoveryKey)

    rx.tryFlush()

    let [head, core] = await Promise.all([headPromise, corePromise])

    if (head === null) {
      head = {
        version: 0,
        total: 0,
        next: 0,
        seed: null
      }
    }

    if (core) return core

    head.total++
    core = { dataPointer: head.next++ }

    tx.putCore(discoveryKey, core)
    tx.setHead(head)

    const ctx = new CoreTX({ dataPointer: core.dataPointer, dependencies: [] }, this.db, tx.updates)

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

    return core
  }

  async create ({ key, manifest, keyPair, encryptionKey, discoveryKey, userData }) {
    const tx = await this._enter()

    let resumed = null

    try {
      resumed = await this._create(tx, { key, manifest, keyPair, encryptionKey, discoveryKey, userData })
    } finally {
      await this._exit()
    }

    return resumed
  }
}

module.exports = CorestoreStorage
