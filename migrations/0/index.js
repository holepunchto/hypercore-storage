const fs = require('fs')
const path = require('path')
const { Readable } = require('streamx')
const b4a = require('b4a')
const flat = require('flat-tree')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const m = require('./messages.js')
const View = require('../../lib/view.js')
const { CorestoreTX, CoreTX, CorestoreRX } = require('../../lib/tx.js')

const EMPTY_NODE = b4a.alloc(40)
const EMPTY_PAGE = b4a.alloc(4096)

let TREE_01_SKIP = null
let TREE_04_SKIP = null
let TREE_16_SKIP = null

class CoreListStream extends Readable {
  constructor (storage) {
    super()

    this.storage = storage
    this.stack = []
  }

  async _open (cb) {
    for (const a of await readdir(path.join(this.storage, 'cores'))) {
      for (const b of await readdir(path.join(this.storage, 'cores', a))) {
        for (const dkey of await readdir(path.join(this.storage, 'cores', a, b))) {
          this.stack.push(path.join(this.storage, 'cores', a, b, dkey))
        }
      }
    }

    cb(null)
  }

  async _read (cb) {
    while (true) {
      const next = this.stack.pop()
      if (!next) {
        this.push(null)
        break
      }

      const oplog = path.join(next, 'oplog')
      const result = await readOplog(oplog)
      if (!result) continue

      this.push(result)
      break
    }

    cb(null)
  }
}

function decodeOplogHeader (state) {
  c.uint32.decode(state) // cksum, ignore for now

  const l = c.uint32.decode(state)
  const length = l >> 2
  const headerBit = l & 1
  const partialBit = l & 2

  if (state.end - state.start < length) return null

  const end = state.start + length
  const result = { header: headerBit, partial: partialBit !== 0, byteLength: length + 8, message: null }

  try {
    result.message = m.oplog.header.decode({ start: state.start, end, buffer: state.buffer })
  } catch {
    return null
  }

  state.start = end
  return result
}

function decodeOplogEntry (state) {
  if (state.end - state.start < 8) return null

  c.uint32.decode(state) // cksum, ignore for now

  const l = c.uint32.decode(state)
  const length = l >>> 2
  const headerBit = l & 1
  const partialBit = l & 2

  if (state.end - state.start < length) return null

  const end = state.start + length

  const result = { header: headerBit, partial: partialBit !== 0, byteLength: length + 8, message: null }

  try {
    result.message = m.oplog.entry.decode({ start: state.start, end, buffer: state.buffer })
  } catch {
    return null
  }

  state.start = end

  return result
}

module.exports = { store, core }

async function store (storage, { version, dryRun = true, gc = true }) {
  const stream = new CoreListStream(storage.path)
  const view = new View()

  const tx = new CorestoreTX(view)
  const head = await storage._getHead(view)
  const primaryKeyFile = path.join(storage.path, 'primary-key')

  const primaryKey = await readFile(primaryKeyFile)

  if (!head.seed) head.seed = primaryKey

  for await (const data of stream) {
    const key = data.header.key
    const discoveryKey = crypto.discoveryKey(data.header.key)
    const files = getFiles(data.path)

    if (head.defaultDiscoveryKey === null) head.defaultDiscoveryKey = discoveryKey

    const core = {
      version: 0, // need later migration
      corePointer: head.allocated.cores++,
      dataPointer: head.allocated.datas++,
      alias: null
    }

    const ptr = { version: 0, corePointer: core.corePointer, dataPointer: core.dataPointer, dependencies: [] }
    const ctx = new CoreTX(ptr, storage.db, view, [])
    const userData = new Map()
    const treeNodes = new Map()

    const auth = {
      key,
      discoveryKey,
      manifest: data.header.manifest,
      keyPair: data.header.keyPair,
      encryptionKey: null
    }

    const tree = {
      length: 0,
      fork: 0,
      rootHash: null,
      signature: null
    }

    if (data.header.tree && data.header.tree.length) {
      tree.length = data.header.tree.length
      tree.fork = data.header.tree.fork
      tree.rootHash = data.header.tree.rootHash
      tree.signature = data.header.tree.signature
    }

    for (const { key, value } of data.header.userData) {
      userData.set(key, value)
    }

    for (const e of data.entries) {
      if (e.userData) userData.set(e.userData.key, e.userData.value)

      if (e.treeNodes) {
        for (const node of e.treeNodes) {
          treeNodes.set(node.index, node)
          ctx.putTreeNode(node)
        }
      }

      if (e.treeUpgrade) {
        if (e.treeUpgrade.ancestors !== tree.length) {
          throw new Error('Unflushed truncations not migrate-able atm')
        }

        tree.length = e.treeUpgrade.length
        tree.fork = e.treeUpgrade.fork
        tree.rootHash = null
        tree.signature = e.treeUpgrade.signature
      }
    }

    if (userData.has('corestore/name') && userData.has('corestore/namespace')) {
      core.alias = {
        name: b4a.toString(userData.get('corestore/name')),
        namespace: userData.get('corestore/namespace')
      }
      userData.delete('corestore/name')
      userData.delete('corestore/namespace')
    }

    for (const [key, value] of userData) {
      ctx.putUserData(key, value)
    }

    ctx.setAuth(auth)

    const getTreeNode = (index) => (treeNodes.get(index) || getTreeNodeFromFile(files.tree, index))

    if (tree.length) {
      if (tree.rootHash === null) tree.rootHash = crypto.tree(await getRoots(tree.length, getTreeNode))
      ctx.setHead(tree)
    }

    tx.putCore(discoveryKey, core)
    if (core.alias) tx.putCoreByAlias(core.alias, discoveryKey)

    await ctx.flush()
  }

  head.version = version
  tx.setHead(head)
  tx.apply()

  if (dryRun) return

  await View.flush(view.changes, storage.db)

  if (gc) await rm(primaryKeyFile)
}

class BlockSlicer {
  constructor (filename) {
    this.stream = fs.createReadStream(filename)
    this.closed = new Promise(resolve => this.stream.once('close', resolve))
    this.offset = 0
    this.overflow = null
  }

  async take (offset, size) {
    let buffer = null
    if (offset < this.offset) throw new Error('overread')

    while (true) {
      let data = null

      if (this.overflow) {
        data = this.overflow
        this.overflow = null
      } else {
        data = this.stream.read()

        if (!data) {
          await new Promise(resolve => this.stream.once('readable', resolve))
          continue
        }
      }

      let chunk = null

      if (this.offset === offset || buffer) {
        chunk = data
      } else if (this.offset + data.byteLength > offset) {
        chunk = data.subarray(offset - this.offset)
      }

      this.offset += data.byteLength
      if (!chunk) continue

      if (buffer) buffer = b4a.concat([buffer, chunk])
      else buffer = chunk

      if (buffer.byteLength < size) continue

      const result = buffer.subarray(0, size)
      this.overflow = size === buffer.byteLength ? null : buffer.subarray(result.byteLength)
      this.offset -= (this.overflow ? this.overflow.byteLength : 0)
      return result
    }
  }

  close () {
    this.stream.on('error', noop)
    this.stream.destroy()
    return this.closed
  }
}

class TreeSlicer {
  constructor () {
    this.buffer = null
    this.offset = 0
  }

  get size () {
    return this.buffer === null ? 0 : this.buffer.byteLength
  }

  push (data) {
    if (this.buffer === null) this.buffer = data
    else this.buffer = b4a.concat([this.buffer, data])
    this.offset += data.byteLength
  }

  skip () {
    let skipped = 0

    if (TREE_01_SKIP === null) {
      TREE_16_SKIP = b4a.alloc(16 * 40 * 100)
      TREE_04_SKIP = TREE_16_SKIP.subarray(0, 4 * 40 * 100)
      TREE_01_SKIP = TREE_16_SKIP.subarray(0, 1 * 40 * 100)
    }

    while (true) {
      if (this.buffer.byteLength >= TREE_16_SKIP.byteLength) {
        if (b4a.equals(this.buffer.subarray(0, TREE_16_SKIP.byteLength), TREE_16_SKIP)) {
          this.buffer = this.buffer.subarray(TREE_16_SKIP.byteLength)
          skipped += 1600
          continue
        }
      }

      if (this.buffer.byteLength >= TREE_04_SKIP.byteLength) {
        if (b4a.equals(this.buffer.subarray(0, TREE_04_SKIP.byteLength), TREE_04_SKIP)) {
          this.buffer = this.buffer.subarray(TREE_04_SKIP.byteLength)
          skipped += 400
          continue
        }
      }

      if (this.buffer.byteLength >= TREE_01_SKIP.byteLength) {
        if (b4a.equals(this.buffer.subarray(0, TREE_01_SKIP.byteLength), TREE_01_SKIP)) {
          this.buffer = this.buffer.subarray(TREE_01_SKIP.byteLength)
          skipped += 100
          continue
        }
      }
      break
    }

    return skipped
  }

  take () {
    const len = 40

    if (len <= this.size) {
      const chunk = this.buffer.subarray(0, len)
      this.buffer = this.buffer.subarray(len)
      return chunk
    }

    return null
  }
}

async function core (core, { version, dryRun = true, gc = true }) {
  if (dryRun) return // dryRun mode not supported atm

  const rx = core.read()

  const promises = [rx.getAuth(), rx.getHead()]
  rx.tryFlush()

  const [auth, head] = await Promise.all(promises)

  if (!auth) return

  const dk = b4a.toString(auth.discoveryKey, 'hex')
  const files = getFiles(path.join(core.store.path, 'cores', dk.slice(0, 2), dk.slice(2, 4), dk))

  if (head === null || head.length === 0) {
    await commitCoreMigration(auth, core, version)
    if (gc) await runGC()
    return // no data
  }

  const oplog = await readOplog(files.oplog)
  if (!oplog) {
    const writable = !!auth.keyPair

    if (writable) {
      throw new Error('No oplog available writable core for ' + files.oplog + ', length = ' + (head ? head.length : 0))
    }

    // if not writable, just nuke it to recover, some bad state happened here, prop corruption from earlier versions
    const w = core.write()

    w.deleteBlockRange(0, -1)
    w.deleteTreeNodeRange(0, -1)
    w.deleteBitfieldPageRange(0, -1)
    w.deleteHead()

    await w.flush()

    await commitCoreMigration(auth, core, version)
    if (gc) await runGC()
    return // no data
  }

  const treeData = new TreeSlicer()

  let treeIndex = 0

  if (await exists(files.tree)) {
    for await (const data of fs.createReadStream(files.tree)) {
      treeData.push(data)

      let write = null

      while (true) {
        const skip = treeData.skip()
        treeIndex += skip

        const buf = treeData.take()
        if (buf === null) break

        const index = treeIndex++
        if (b4a.equals(buf, EMPTY_NODE)) continue

        if (write === null) write = core.write()
        write.putTreeNode(decodeTreeNode(index, buf))
      }

      if (write !== null) await write.flush()
    }
  }

  const buf = []
  if (await exists(files.bitfield)) {
    for await (const data of fs.createReadStream(files.bitfield)) {
      buf.push(data)
    }
  }

  let bitfield = b4a.concat(buf)
  if (bitfield.byteLength & 4095) bitfield = b4a.concat([bitfield, b4a.alloc(4096 - (bitfield.byteLength & 4095))])

  const pages = new Map()
  const headerBits = new Map()

  const roots = await getRootsFromStorage(core, head.length)

  for (const e of oplog.entries) {
    if (!e.bitfield) continue

    for (let i = 0; i < e.bitfield.length; i++) {
      headerBits.set(i + e.bitfield.start, !e.bitfield.drop)
    }
  }

  let batch = []

  const cache = new Map()
  const blocks = new BlockSlicer(files.data)

  for (const index of allBits(bitfield)) {
    if (headerBits.get(index) === false) continue
    if (index >= head.length) continue

    setBitInPage(index)

    batch.push(index)
    if (batch.length < 1024) continue

    await writeBlocksBatch()
    continue
  }

  if (batch.length) await writeBlocksBatch()

  await blocks.close()

  const w = core.write()

  for (const [index, bit] of headerBits) {
    if (!bit) continue
    if (index >= head.length) continue

    setBitInPage(index)

    const blk = await getBlockFromFile(files.data, core, index, roots, cache)
    w.putBlock(index, blk)
  }

  for (const [index, page] of pages) {
    w.putBitfieldPage(index, b4a.from(page.buffer, page.byteOffset, page.byteLength))
  }

  await w.flush()

  let contiguousLength = 0
  for await (const data of core.createBlockStream()) {
    if (data.index === contiguousLength) contiguousLength++
    else break
  }

  if (contiguousLength) {
    const w = core.write()
    w.setHints({ contiguousLength })
    await w.flush()
  }

  await commitCoreMigration(auth, core, version)

  if (gc) await runGC()

  async function runGC () {
    await rm(files.path)
    await rmdir(path.join(files.path, '..'))
    await rmdir(path.join(files.path, '../..'))
    await rmdir(path.join(core.store.path, 'cores'))
  }

  function setBitInPage (index) {
    const n = index & 32767
    const p = (index - n) / 32768

    let page = pages.get(p)

    if (!page) {
      page = new Uint32Array(1024)
      pages.set(p, page)
    }

    const o = n & 31
    const b = (n - o) / 32
    const v = 1 << o

    page[b] |= v
  }

  async function writeBlocksBatch () {
    const read = core.read()
    const promises = []
    for (const index of batch) promises.push(getByteRangeFromStorage(read, 2 * index, roots, cache))
    read.tryFlush()

    const r = await Promise.all(promises)
    const tx = core.write()

    for (let i = 0; i < r.length; i++) {
      const index = batch[i]
      const [offset, size] = r[i]

      const blk = await blocks.take(offset, size)
      tx.putBlock(index, blk)
    }

    batch = []
    if (cache.size > 16384) cache.clear()

    await tx.flush()
  }
}

async function commitCoreMigration (auth, core, version) {
  const view = new View()
  const rx = new CorestoreRX(core.db, view)

  const storeCorePromise = rx.getCore(auth.discoveryKey)
  rx.tryFlush()

  const storeCore = await storeCorePromise

  storeCore.version = version

  const tx = new CorestoreTX(view)

  tx.putCore(auth.discoveryKey, storeCore)
  tx.apply()

  await View.flush(view.changes, core.db)
}

async function getBlockFromFile (file, core, index, roots, cache) {
  const rx = core.read()
  const promise = getByteRangeFromStorage(rx, 2 * index, roots, cache)
  rx.tryFlush()
  const [offset, size] = await promise

  return new Promise(function (resolve) {
    readAll(file, size, offset, function (err, buf) {
      if (err) return resolve(null)
      resolve(buf)
    })
  })
}

function getFiles (dir) {
  return {
    path: dir,
    oplog: path.join(dir, 'oplog'),
    data: path.join(dir, 'data'),
    tree: path.join(dir, 'tree'),
    bitfield: path.join(dir, 'bitfield')
  }
}

async function getRootsFromStorage (core, length) {
  const all = []
  const rx = core.read()
  for (const index of flat.fullRoots(2 * length)) {
    all.push(rx.getTreeNode(index))
  }
  rx.tryFlush()
  return Promise.all(all)
}

async function getRoots (length, getTreeNode) {
  const all = []
  for (const index of flat.fullRoots(2 * length)) {
    all.push(await getTreeNode(index))
  }
  return all
}

function getCached (read, cache, index) {
  if (cache.has(index)) return cache.get(index)
  const p = read.getTreeNode(index)
  cache.set(index, p)
  return p
}

async function getByteRangeFromStorage (read, index, roots, cache) {
  const promises = [getCached(read, cache, index), getByteOffsetFromStorage(read, index, roots, cache)]
  const [node, offset] = await Promise.all(promises)
  return [offset, node.size]
}

async function getByteOffsetFromStorage (rx, index, roots, cache) {
  if (index === 0) return 0
  if ((index & 1) === 1) index = flat.leftSpan(index)

  let head = 0
  let offset = 0

  for (const node of roots) { // all async ticks happen once we find the root so safe
    head += 2 * ((node.index - head) + 1)

    if (index >= head) {
      offset += node.size
      continue
    }

    const ite = flat.iterator(node.index)
    const promises = []

    while (ite.index !== index) {
      if (index < ite.index) {
        ite.leftChild()
      } else {
        promises.push(getCached(rx, cache, ite.leftChild()))
        ite.sibling()
      }
    }

    const nodes = await Promise.all(promises)
    for (const node of nodes) offset += node.size

    return offset
  }

  throw new Error('Failed to find offset')
}

function decodeTreeNode (index, buf) {
  return { index, size: c.decode(c.uint64, buf), hash: buf.subarray(8) }
}

async function getTreeNodeFromFile (file, index) {
  return new Promise(function (resolve) {
    readAll(file, 40, index * 40, function (err, buf) {
      if (err) return resolve(null)
      resolve(decodeTreeNode(index, buf))
    })
  })
}

function readAll (filename, length, pos, cb) {
  const buf = b4a.alloc(length)

  fs.open(filename, 'r', function (err, fd) {
    if (err) return cb(err)

    let offset = 0

    fs.read(fd, buf, offset, buf.byteLength, pos, function loop (err, read) {
      if (err) return done(err)
      if (read === 0) return done(new Error('Partial read'))
      offset += read
      if (offset === buf.byteLength) return done(null, buf)
      fs.read(fd, offset, buf.byteLength - offset, buf, pos + offset, loop)
    })

    function done (err, value) {
      fs.close(fd, () => cb(err, value))
    }
  })
}

async function readdir (dir) {
  try {
    return await fs.promises.readdir(dir)
  } catch {
    return []
  }
}

async function exists (file) {
  try {
    await fs.promises.stat(file)
    return true
  } catch {
    return false
  }
}

async function readFile (file) {
  try {
    return await fs.promises.readFile(file)
  } catch {
    return null
  }
}

async function rm (dir) {
  try {
    await fs.promises.rm(dir, { recursive: true })
  } catch {}
}

async function rmdir (dir) {
  try {
    await fs.promises.rmdir(dir)
  } catch {}
}

function * allBits (buffer) {
  for (let i = 0; i < buffer.byteLength; i += EMPTY_PAGE.byteLength) {
    const page = buffer.subarray(i, i + EMPTY_NODE.byteLength)
    if (b4a.equals(page, EMPTY_PAGE)) continue

    const view = new Uint32Array(page.buffer, page.byteOffset, EMPTY_PAGE.byteLength / 4)

    for (let j = 0; j < view.length; j++) {
      const n = view[j]
      if (n === 0) continue

      for (let k = 0; k < 32; k++) {
        const m = 1 << k
        if (n & m) yield i * 8 + j * 32 + k
      }
    }
  }
}

function readOplog (oplog) {
  return new Promise(function (resolve) {
    fs.readFile(oplog, function (err, buffer) {
      if (err) return resolve(null)

      const state = { start: 0, end: buffer.byteLength, buffer }
      const headers = [1, 0]

      const h1 = decodeOplogHeader(state)
      state.start = 4096

      const h2 = decodeOplogHeader(state)
      state.start = 4096 * 2

      if (!h1 && !h2) return resolve(null)

      if (h1 && !h2) {
        headers[0] = h1.header
        headers[1] = h1.header
      } else if (!h1 && h2) {
        headers[0] = (h2.header + 1) & 1
        headers[1] = h2.header
      } else {
        headers[0] = h1.header
        headers[1] = h2.header
      }

      const header = (headers[0] + headers[1]) & 1
      const result = { path: path.dirname(oplog), header: null, entries: [] }
      const decoded = []

      result.header = header ? h2.message : h1.message

      if (result.header.external) {
        fs.readFile(path.join(oplog, '../header'), function (err, buffer) {
          if (err) return resolve(null)
          const start = result.header.external.start
          const end = start + result.header.external.length
          result.header = m.oplog.header.decode({ buffer, start, end })
          finish()
        })
        return
      }

      finish()

      function finish () {
        while (true) {
          const entry = decodeOplogEntry(state)
          if (!entry) break
          if (entry.header !== header) break

          decoded.push(entry)
        }

        while (decoded.length > 0 && decoded[decoded.length - 1].partial) decoded.pop()

        for (const e of decoded) {
          result.entries.push(e.message)
        }

        resolve(result)
      }
    })
  })
}

function noop () {}
