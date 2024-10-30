const { ASSERTION } = require('hypercore-errors')

const TipList = require('./tip-list')

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
  }

  async registerBatch (name, length, overwrite) {
    todo()
  }

  snapshot () {
    todo()
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

  createBitfieldPageStream () {
    todo()
  }

  async peekLastTreeNode () {
    const mem = this.treeNodes === null ? null : findHighest(this.treeNodes)

    const node = mem.valid ? mem.value : null
    const disk = await this.storage.peekLastTreeNode()

    return (node && (!disk || node.index > disk.index)) ? node : disk
  }

  async peekLastBitfieldPage () {
    const mem = this.bitfields !== null ? this.bitfields.get(this.bitfields.length() - 1) : { valid: false }

    const page = mem.valid ? mem.value : null
    const index = page ? this.bitfields.length() - 1 : -1

    const disk = await this.storage.peekLastBitfieldPage()

    return (page && (!disk || index > disk.index)) ? { index, page } : disk
  }

  close () {
    return Promise.resolve()
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

function todo () {
  throw ASSERTION('Not supported yet, but will be')
}
