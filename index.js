const RocksDB = require('rocksdb-native')
const rrp = require('resolve-reject-promise')
const ScopeLock = require('scope-lock')
const View = require('./lib/view.js')

const VERSION = 1

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
  createUserDataStream,
  createTreeNodeStream
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

    const promises = []
    while (this.flushes.length) promises.push(this.flushes.pop()())

    return Promise.all(promises)
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

  get snapshotted () {
    return this.db._snapshot !== null
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

  createBlockStream (opts) {
    return createBlockStream(this.core, this.db, this.view, opts)
  }

  createTreeNodeStream (opts) {
    return createTreeNodeStream(this.core, this.db, this.view, opts)
  }

  createBitfieldStream (opts) {
    return createBitfieldStream(this.core, this.db, this.view, opts)
  }

  createUserDataStream (opts) {
    return createUserDataStream(this.core, this.db, this.view, opts)
  }

  async resumeSession (name) {
    const rx = this.read()
    const existingSessionsPromise = rx.getSessions()

    rx.tryFlush()
    const existingSessions = await existingSessionsPromise

    const sessions = existingSessions || []
    const session = getBatch(sessions, name, false)

    if (session === null) return null

    const core = {
      version: this.core.version,
      corePointer: this.core.corePointer,
      dataPointer: session.dataPointer,
      dependencies: []
    }

    const coreRx = new CoreRX(core, this.db, this.view)

    const dependencyPromise = coreRx.getDependency()
    coreRx.tryFlush()

    const dependency = await dependencyPromise
    if (dependency) core.dependencies = this._addDependency(dependency)

    return new HypercoreStorage(this.store, this.db.session(), core, this.atomic ? this.view : new View(), this.atomic)
  }

  async createSession (name, head, atom) {
    const rx = this.read(atom)

    const existingSessionsPromise = rx.getSessions()
    const existingHeadPromise = rx.getHead()

    rx.tryFlush()

    const [existingSessions, existingHead] = await Promise.all([existingSessionsPromise, existingHeadPromise])
    if (head === null) head = existingHead

    if (existingHead !== null && head.length > existingHead.length) {
      throw new Error('Invalid head passed, ahead of core')
    }

    const sessions = existingSessions || []
    const session = getBatch(sessions, name, true)

    if (session.dataPointer === -1) {
      session.dataPointer = await this.store._allocData()
    }

    const tx = this.write(atom)

    tx.setSessions(sessions)

    const length = head === null ? 0 : head.length
    const core = {
      version: this.core.version,
      corePointer: this.core.corePointer,
      dataPointer: session.dataPointer,
      dependencies: this._addDependency({ dataPointer: this.core.dataPointer, length })
    }

    const coreTx = new CoreTX(core, this.db, tx.view, tx.changes)

    if (length > 0) coreTx.setHead(head)
    coreTx.setDependency(core.dependencies[core.dependencies.length - 1])

    await tx.flush()

    return new HypercoreStorage(this.store, this.db.session(), core, atom ? atom.view : this.atomic ? this.view : new View(), !!atom || this.atomic)
  }

  async createAtomicSession (atom, head) {
    const length = head === null ? 0 : head.length
    const core = {
      version: this.core.version,
      corePointer: this.core.corePointer,
      dataPointer: this.core.dataPointer,
      dependencies: this._addDependency({ dataPointer: this.core.dataPointer, length })
    }

    const coreTx = new CoreTX(core, this.db, atom.view, [])

    if (length > 0) coreTx.setHead(head)
    coreTx.setDependency(core.dependencies[core.dependencies.length - 1])

    await coreTx.flush()

    return this.atomize(atom)
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

  read (atom) {
    return new CoreRX(this.core, this.db, atom ? atom.view : this.view)
  }

  write (atom) {
    return new CoreTX(this.core, this.db, atom ? atom.view : this.atomic ? this.view : null, [])
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
    this.path = typeof db === 'string' ? db : db.path
    this.db = typeof db === 'string' ? new RocksDB(db) : db
    this.view = null
    this.enters = 0
    this.lock = new ScopeLock()
    this.flushing = null
    this.version = 0
    this.migrating = null
  }

  get opened () {
    return this.db.opened
  }

  get closed () {
    return this.db.closed
  }

  async ready () {
    if (this.version === 0) await this._migrateStore()
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

  // runs pre any other mutation and read
  async _migrateStore () {
    const view = await this._enter()

    try {
      if (this.version === VERSION) return

      const rx = new CorestoreRX(this.db, view)
      const headPromise = rx.getHead()

      rx.tryFlush()
      const head = await headPromise

      const version = head === null ? 0 : head.version
      if (version === VERSION) return

      const target = { version: VERSION, dryRun: false }

      switch (version) {
        case 0: {
          await require('./migrations/0').store(this, target)
          break
        }
        default: {
          throw new Error('Unsupported version: ' + version + ' - you should probably upgrade your dependencies')
        }
      }

      this.version = VERSION
    } finally {
      await this._exit()
    }
  }

  // runs pre the core is returned to the user
  async _migrateCore (core, discoveryKey, locked) {
    const view = locked ? this.view : await this._enter()

    const version = core.core.version

    try {
      if (version === VERSION) return

      const target = { version: VERSION, dryRun: false }

      switch (version) {
        case 0: {
          await require('./migrations/0').core(core, target)
          break
        }
        default: {
          throw new Error('Unsupported version: ' + version + ' - you should probably upgrade your dependencies')
        }
      }

      core.core.version = VERSION

      if (locked === false) return

      // if its locked, then move the core state into the memview
      // in case the core is reopened from the memview, pre flush

      const rx = new CorestoreRX(this.db, EMPTY)
      const tx = new CorestoreTX(view)

      const corePromise = rx.getCore(discoveryKey)
      rx.tryFlush()

      tx.putCore(discoveryKey, await corePromise)
      tx.apply()
    } finally {
      if (!locked) await this._exit()
    }
  }

  async _enter () {
    this.enters++
    await this.lock.lock()
    if (this.view === null) this.view = new View()
    return this.view
  }

  async _exit () {
    this.enters--

    if (this.flushing === null) this.flushing = rrp()
    const flushed = this.flushing.promise

    if (this.enters === 0 || this.view.size() > 128) {
      try {
        await View.flush(this.view.changes, this.db)
        this.flushing.resolve()
      } catch (err) {
        this.flushing.reject(err)
      } finally {
        this.flushing = null
        this.view = null
      }
    }

    this.lock.unlock()
    return flushed
  }

  // when used with core catches this isnt transactional for simplicity, HOWEVER, its just a number
  // so worth the tradeoff
  async _allocData () {
    let dataPointer = 0

    const view = await this._enter()
    const tx = new CorestoreTX(view)

    try {
      const head = await this._getHead(view)

      dataPointer = head.allocated.datas++

      tx.setHead(head)
      tx.apply()
    } finally {
      await this._exit()
    }

    return dataPointer
  }

  // exposes here so migrations can easily access the head in an init state
  async _getHead (view) {
    const rx = new CorestoreRX(this.db, view)
    const headPromise = rx.getHead()
    rx.tryFlush()

    const head = await headPromise
    return head === null ? initStoreHead() : head
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
    if (this.version === 0) await this._migrateStore()

    const view = await this._enter()
    const tx = new CorestoreTX(view)

    tx.clear()
    tx.apply()

    await this._exit()
  }

  createCoreStream () {
    // TODO: be nice to run the mgiration here also, but too much plumbing atm
    return createCoreStream(this.db, EMPTY)
  }

  createAliasStream (namespace) {
    // TODO: be nice to run the mgiration here also, but too much plumbing atm
    return createAliasStream(this.db, EMPTY, namespace)
  }

  async getAlias (alias) {
    if (this.version === 0) await this._migrateStore()

    const rx = new CorestoreRX(this.db, EMPTY)
    const discoveryKeyPromise = rx.getCoreByAlias(alias)
    rx.tryFlush()
    return discoveryKeyPromise
  }

  async getSeed () {
    if (this.version === 0) await this._migrateStore()

    const rx = new CorestoreRX(this.db, EMPTY)
    const headPromise = rx.getHead()

    rx.tryFlush()

    const head = await headPromise
    return head === null ? null : head.seed
  }

  async setSeed (seed, { overwrite = true } = {}) {
    if (this.version === 0) await this._migrateStore()

    const view = await this._enter()
    const tx = new CorestoreTX(view)

    try {
      const rx = new CorestoreRX(this.db, view)
      const headPromise = rx.getHead()

      rx.tryFlush()

      const head = (await headPromise) || initStoreHead()

      if (head.seed === null || overwrite) head.seed = seed
      tx.setHead(head)
      tx.apply()

      return head.seed
    } finally {
      await this._exit()
    }
  }

  async getDefaultDiscoveryKey () {
    if (this.version === 0) await this._migrateStore()

    const rx = new CorestoreRX(this.db, EMPTY)
    const headPromise = rx.getHead()

    rx.tryFlush()

    const head = await headPromise
    return head === null ? null : head.defaultDiscoveryKey
  }

  async setDefaultDiscoveryKey (discoveryKey, { overwrite = true } = {}) {
    if (this.version === 0) await this._migrateStore()

    const view = await this._enter()
    const tx = new CorestoreTX(view)

    try {
      const rx = new CorestoreRX(this.db, view)
      const headPromise = rx.getHead()

      rx.tryFlush()

      const head = (await headPromise) || initStoreHead()

      if (head.defaultDiscoveryKey === null || overwrite) head.defaultDiscoveryKey = discoveryKey
      tx.setHead(head)
      tx.apply()

      return head.defaultDiscoveryKey
    } finally {
      await this._exit()
    }
  }

  async has (discoveryKey) {
    if (this.version === 0) await this._migrateStore()

    const rx = new CorestoreRX(this.db, EMPTY)
    const promise = rx.getCore(discoveryKey)

    rx.tryFlush()

    return (await promise) !== null
  }

  async resume (discoveryKey) {
    if (this.version === 0) await this._migrateStore()

    if (!discoveryKey) {
      discoveryKey = await this.getDefaultDiscoveryKey()
      if (!discoveryKey) return null
    }

    const rx = new CorestoreRX(this.db, EMPTY)
    const corePromise = rx.getCore(discoveryKey)

    rx.tryFlush()
    const core = await corePromise

    if (core === null) return null
    return this._resumeFromPointers(EMPTY, discoveryKey, false, core)
  }

  async _resumeFromPointers (view, discoveryKey, create, { version, corePointer, dataPointer }) {
    const core = { version, corePointer, dataPointer, dependencies: [] }

    while (true) {
      const rx = new CoreRX({ version, dataPointer, corePointer: 0, dependencies: [] }, this.db, view)
      const dependencyPromise = rx.getDependency()
      rx.tryFlush()
      const dependency = await dependencyPromise
      if (!dependency) break
      core.dependencies.push(dependency)
      dataPointer = dependency.dataPointer
    }

    const result = new HypercoreStorage(this, this.db.session(), core, EMPTY, false)

    if (result.core.version === 0) await this._migrateCore(result, discoveryKey, create)
    return result
  }

  // not allowed to throw validation errors as its a shared tx!
  async _create (view, { key, manifest, keyPair, encryptionKey, discoveryKey, alias, userData }) {
    const rx = new CorestoreRX(this.db, view)
    const tx = new CorestoreTX(view)

    const corePromise = rx.getCore(discoveryKey)
    const headPromise = rx.getHead()

    rx.tryFlush()

    let [core, head] = await Promise.all([corePromise, headPromise])
    if (core) return this._resumeFromPointers(view, discoveryKey, true, core)

    if (head === null) head = initStoreHead()
    if (head.defaultDiscoveryKey === null) head.defaultDiscoveryKey = discoveryKey

    const corePointer = head.allocated.cores++
    const dataPointer = head.allocated.datas++

    core = { version: VERSION, corePointer, dataPointer, alias }

    tx.setHead(head)
    tx.putCore(discoveryKey, core)
    if (alias) tx.putCoreByAlias(alias, discoveryKey)

    const ptr = { corePointer, dataPointer, dependencies: [] }
    const ctx = new CoreTX(ptr, this.db, view, tx.changes)

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

    tx.apply()

    return new HypercoreStorage(this, this.db.session(), ptr, EMPTY, false)
  }

  async create (data) {
    if (this.version === 0) await this._migrateStore()

    const view = await this._enter()

    try {
      return await this._create(view, data)
    } finally {
      await this._exit()
    }
  }
}

module.exports = CorestoreStorage

function initStoreHead () {
  return {
    version: 0, // cause we wanna run the migration
    allocated: {
      datas: 0,
      cores: 0
    },
    seed: null,
    defaultDiscoveryKey: null
  }
}

function getBatch (sessions, name, alloc) {
  for (let i = 0; i < sessions.length; i++) {
    if (sessions[i].name === name) return sessions[i]
  }

  if (!alloc) return null

  const result = { name, dataPointer: -1 }
  sessions.push(result)
  return result
}

function isCorestoreStorage (s) {
  return typeof s === 'object' && !!s && typeof s.setDefaultDiscoveryKey === 'function'
}
