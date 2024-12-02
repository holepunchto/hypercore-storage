const { ASSERTION } = require('hypercore-errors')
const { Readable, isEnded, getStreamError } = require('streamx')

const TipList = require('./tip-list')

class OverlayStream extends Readable {
  constructor (tip, storage, createStream, opts = {}) {
    super()

    this.tip = tip
    this.storage = storage
    this.options = opts

    this._reverse = !!opts.reverse
    this._active = null
    this._position = this._reverse ? tip.length() - 1 : tip.offset

    this._createStream = createStream
    this._pendingDestroy = null

    if (!opts.reverse) this._makeActiveStream()
  }

  _predestroy () {
    if (this._active) this._active.destroy()
  }

  _read (cb) {
    if (this._active === null) {
      if (this._yield()) {
        cb(null)
        return
      }
    }

    if (this._active) {
      this._active.resume()
    }

    cb(null)
  }

  _yield () {
    while (this._memoryRemaining()) {
      const { valid, value } = this.tip.get(this._position)
      const index = this._nextIndex()

      if (!valid) continue
      if (!this.push({ index, page: value })) break
    }

    return this._onmemorydrain()
  }

  _nextIndex () {
    if (this._reverse) return this._position--
    return this._position++
  }

  _memoryRemaining () {
    if (this._reverse) return this._position >= this.tip.offset
    return this._position < this.tip.length()
  }

  _onmemorydrain () {
    if (!this._reverse) {
      this.push(null)
      return true
    }

    this._makeActiveStream()

    return false
  }

  _makeActiveStream () {
    this._active = this._createStream(this.storage, this.options)
    this._active.on('error', noop) // handled in onclose
    this._active.on('close', this._onclose.bind(this))
    this._active.on('data', this._ondata.bind(this))
  }

  _ondata (data) {
    if (!this.push(data)) this._active.pause()
  }

  _onclose () {
    if (!isEnded(this._active)) {
      const error = getStreamError(this._active)
      this.destroy(error)
      return
    }

    if (this._reverse) {
      this.push(null)
      return
    }

    this._yield()
  }
}

class MemoryOverlay {
  constructor (storage) {
    this.storage = storage
    this.head = null
    this.auth = null
    this.localKeyPair = null
    this.encryptionKey = null
    this.dataDependency = null
    this.dataInfo = null
    this.userData = null
    this.blocks = null
    this.treeNodes = null
    this.bitfields = null

    this.snapshotted = false
    this.snapshotShared = null
    this.activeSnapshots = 0
    this.copyOnWrite = false
  }

  async registerBatch (name, length, overwrite) {
    todo()
  }

  snapshot () {
    if (this.snapshotShared !== null) return this.snapshotShared.snapshot()

    this.activeSnapshots++
    this.copyOnWrite = true

    const snap = new MemoryOverlay(this.storage.snapshot())

    snap.snapshotted = true
    snap.snapshotShared = this

    if (this.head !== null) snap.head = this.head
    if (this.auth !== null) snap.auth = this.auth
    if (this.localKeyPair !== null) snap.localKeyPair = this.localKeyPair
    if (this.encryptionKey !== null) snap.encryptionKey = this.encryptionKey
    if (this.dataDependency !== null) snap.dataDependency = this.dataDependency
    if (this.dataInfo !== null) snap.dataInfo = this.dataInfo
    if (this.userData !== null) snap.userData = this.userData
    if (this.blocks !== null) snap.blocks = this.blocks
    if (this.treeNodes !== null) snap.treeNodes = this.treeNodes
    if (this.bitfields !== null) snap.bitfields = this.bitfields

    return snap
  }

  _copy () {
    this.copyOnWrite = false

    // clone state
    if (this.head !== null) this.head = Object.assign(this.head)
    if (this.auth !== null) this.auth = Object.assign(this.auth)
    if (this.localKeyPair !== null) this.localKeyPair = Object.assign(this.localKeyPair)
    if (this.encryptionKey !== null) this.encryptionKey = Object.assign(this.encryptionKey)
    if (this.dataDependency !== null) this.dataDependency = Object.assign(this.dataDependency)
    if (this.dataInfo !== null) this.dataInfo = Object.assign(this.dataInfo)
    if (this.userData !== null) this.userData = mergeMap(new Map(), this.userData)
    if (this.blocks !== null) this.blocks = mergeTip(new TipList(), this.blocks)
    if (this.treeNodes !== null) this.treeNodes = mergeMap(new Map(), this.treeNodes)
    if (this.bitfields !== null) this.bitfields = mergeTip(new TipList(), this.bitfields)
  }

  get dependencies () {
    return this.storage.dependencies
  }

  dependencyLength () {
    if (this.dataDependency) return this.dataDependency.length
    return this.storage.dependencyLength()
  }

  createReadBatch () {
    return new MemoryOverlayReadBatch(this, this.storage.createReadBatch())
  }

  createWriteBatch () {
    if (this.copyOnWrite) this._copy()
    return new MemoryOverlayWriteBatch(this)
  }

  createBlockStream () {
    todo()
  }

  createUserDataStream () {
    todo()
  }

  createTreeNodeStream () {
    todo()
  }

  createBitfieldPageStream (opts) {
    if (this.bitfields === null) return createBitfieldPageStream(this.storage)
    return new OverlayStream(this.bitfields, this.storage, createBitfieldPageStream, opts)
  }

  findDependency () {
    return null
  }

  async peekLastTreeNode () {
    todo()
  }

  async peekLastBitfieldPage () {
    const mem = this.bitfields !== null ? this.bitfields.get(this.bitfields.length() - 1) : { valid: false }

    const page = mem.valid ? mem.value : null
    const index = page ? this.bitfields.length() - 1 : -1

    const disk = await this.storage.peekLastBitfieldPage()

    return (page && (!disk || index > disk.index)) ? { index, page } : disk
  }

  destroy () {
    if (this.snapshotted) this.storage.destroy()
    if (this.snapshotShared === null) return
    if (--this.snapshotShared.activeSnapshots === 0) this.snapshotShared.copyOnWrite = false
    this.snapshotShared = null
  }

  merge (overlay) {
    if (overlay.head !== null) this.head = overlay.head
    if (overlay.auth !== null) this.auth = overlay.auth
    if (overlay.localKeyPair !== null) this.localKeyPair = overlay.localKeyPair
    if (overlay.encryptionKey !== null) this.encryptionKey = overlay.encryptionKey
    if (overlay.dataDependency !== null) this.dataDependency = overlay.dataDependency
    if (overlay.dataInfo !== null) this.dataInfo = overlay.dataInfo
    if (overlay.userData !== null) this.userData = mergeMap(this.userData, overlay.userData)
    if (overlay.blocks !== null) this.blocks = mergeTip(this.blocks, overlay.blocks)
    if (overlay.treeNodes !== null) this.treeNodes = mergeMap(this.treeNodes, overlay.treeNodes)
    if (overlay.bitfields !== null) this.bitfields = mergeTip(this.bitfields, overlay.bitfields)
  }
}

module.exports = MemoryOverlay

class MemoryOverlayReadBatch {
  constructor (overlay, read) {
    this.read = read
    this.overlay = overlay
  }

  async getCoreHead () {
    return this.overlay.head !== null ? this.overlay.head : this.read.getCoreHead()
  }

  async getCoreAuth () {
    return this.overlay.auth !== null ? this.overlay.auth : this.read.getCoreAuth()
  }

  async getDataDependency () {
    return this.overlay.dataDependency !== null ? this.overlay.dataDependency : this.read.getDataDependency()
  }

  async getLocalKeyPair () {
    return this.overlay.localKeyPair !== null ? this.overlay.localKeyPair : this.read.getLocalKeyPair()
  }

  async getEncryptionKey () {
    return this.overlay.encryptionKey !== null ? this.overlay.encryptionKey : this.read.getEncryptionKey()
  }

  async getDataInfo () {
    return this.overlay.dataInfo !== null ? this.overlay.dataInfo : this.read.getDataInfo()
  }

  async getUserData (key) {
    return this.overlay.userData !== null && this.overlay.userData.has(key)
      ? this.overlay.userData.get(key)
      : this.read.getUserData(key)
  }

  async hasBlock (index) {
    if (this.overlay.blocks !== null && index >= this.overlay.blocks.offset) {
      const blk = this.overlay.blocks.get(index)
      if (blk.valid) return true
    }
    return this.read.hasBlock(index)
  }

  async getBlock (index, error) {
    if (this.overlay.blocks !== null && index >= this.overlay.blocks.offset) {
      const blk = this.overlay.blocks.get(index)
      if (blk.valid) return blk.value
    }
    return this.read.getBlock(index, error)
  }

  async hasTreeNode (index) {
    return this.overlay.treeNodes !== null
      ? this.overlay.treeNodes.has(index)
      : this.read.hasTreeNode(index)
  }

  async getTreeNode (index, error) {
    return this.overlay.treeNodes !== null && this.overlay.treeNodes.has(index)
      ? this.overlay.treeNodes.get(index)
      : this.read.getTreeNode(index, error)
  }

  async getBitfieldPage (index) {
    if (this.overlay.bitfields !== null && index >= this.overlay.bitfields.offset) {
      const page = this.overlay.bitfields.get(index)
      if (page.valid) return page.value
    }
    return this.read.getBitfieldPage(index)
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

class MemoryOverlayWriteBatch {
  constructor (storage) {
    this.storage = storage
    this.overlay = new MemoryOverlay()
  }

  setCoreHead (head) {
    this.overlay.head = head
  }

  setCoreAuth (auth) {
    this.overlay.auth = auth
  }

  setBatchPointer (name, pointer) {
    todo()
  }

  setDataDependency (dependency) {
    this.overlay.dataDependency = dependency
  }

  setLocalKeyPair (keyPair) {
    this.overlay.localKeyPair = keyPair
  }

  setEncryptionKey (encryptionKey) {
    this.overlay.encryptionKey = encryptionKey
  }

  setDataInfo (info) {
    this.overlay.dataInfo = info
  }

  setUserData (key, value) {
    if (this.overlay.userData === null) this.overlay.userData = new Map()
    this.overlay.userData.set(key, value)
  }

  putBlock (index, data) {
    if (this.overlay.blocks === null) this.overlay.blocks = new TipList()
    this.overlay.blocks.put(index, data)
  }

  deleteBlock (index) {
    todo()
  }

  deleteBlockRange (start, end) {
    if (this.overlay.blocks === null) this.overlay.blocks = new TipList()
    this.overlay.blocks.delete(start, end)
  }

  putTreeNode (node) {
    if (this.overlay.treeNodes === null) this.overlay.treeNodes = new Map()
    this.overlay.treeNodes.set(node.index, node)
  }

  deleteTreeNode (index) {
    todo()
  }

  deleteTreeNodeRange (start, end) {
    todo()
  }

  putBitfieldPage (index, page) {
    if (this.overlay.bitfields === null) this.overlay.bitfields = new TipList()
    this.overlay.bitfields.put(index, page)
  }

  deleteBitfieldPage (index) {
    todo()
  }

  deleteBitfieldPageRange (start, end) {
    if (this.overlay.bitfields === null) this.overlay.bitfields = new TipList()
    this.overlay.bitfields.delete(start, end)
  }

  destroy () {}

  flush () {
    this.storage.merge(this.overlay)
    return Promise.resolve()
  }
}

function createBitfieldPageStream (storage, opts) {
  return storage.createBitfieldPageStream(opts)
}

function mergeMap (a, b) {
  if (a === null) return b
  for (const [key, value] of b) a.set(key, value)
  return a
}

function mergeTip (a, b) {
  if (a === null) return b
  a.merge(b)
  return a
}

function noop () {}

function todo () {
  throw ASSERTION('Not supported yet, but will be')
}
