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

  _read (cb) {
    const next = this.stack.pop()
    if (!next) {
      this.push(null)
      cb(null)
      return
    }

    const oplog = path.join(next, 'oplog')
    fs.readFile(oplog, (err, buffer) => {
      if (err) return this._read(cb) // next

      const state = { start: 0, end: buffer.byteLength, buffer }
      const headers = [1, 0]

      const h1 = decodeOplogHeader(state)
      state.start = 4096

      const h2 = decodeOplogHeader(state)
      state.start = 4096 * 2

      if (!h1 && !h2) return this._read(cb)

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
      const result = { path: next, header: null, entries: [] }
      const decoded = []

      result.header = header ? h2.message : h1.message

      if (result.header.external) {
        throw new Error('External headers not migrate-able atm')
      }

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

      this.push(result)

      cb(null)
    })
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

    const blocks = []
    const tree = {
      length: 0,
      fork: 0,
      rootHash: null,
      signature: null
    }

    let contiguousLength = 0

    if (data.header.tree && data.header.tree.length) {
      tree.length = data.header.tree.length
      tree.fork = data.header.tree.fork
      tree.rootHash = data.header.tree.rootHash
      tree.signature = data.header.tree.signature
    }

    if (data.header.hints) {
      contiguousLength = data.header.hints.contiguousLength
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

      if (e.bitfield) {
        if (e.bitfield.drop) {
          throw new Error('Unflushed truncations not migrate-able atm')
        }

        for (let i = e.bitfield.start; i < e.bitfield.start + e.bitfield.length; i++) {
          blocks.push(i)
        }
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
    const roots = tree.rootHash === null || blocks.length > 0 ? await getRoots(tree.length, getTreeNode) : null

    if (tree.length) {
      if (tree.rootHash === null) tree.rootHash = crypto.tree(roots)
      ctx.setHead(tree)
    }

    blocks.sort((a, b) => a - b)

    for (const index of blocks) {
      if (index === contiguousLength) contiguousLength++
      const blk = await getBlockFromFile(files.data, index, roots, getTreeNode)
      ctx.putBlock(index, blk)
    }

    if (contiguousLength > 0) {
      ctx.setHints({ contiguousLength })
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

class Slicer {
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

  take (len) {
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
    if (gc) await runGC()
    return // no data
  }

  const treeData = new Slicer()

  let treeIndex = 0

  if (await exists(files.tree)) {
    for await (const data of fs.createReadStream(files.tree)) {
      treeData.push(data)

      const write = core.write()

      while (true) {
        const buf = treeData.take(40)
        if (buf === null) break

        const index = treeIndex++
        if (b4a.equals(buf, EMPTY_NODE)) continue

        write.putTreeNode(decodeTreeNode(index, buf))
      }

      await write.flush()
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

  for await (const data of core.createBlockStream()) {
    const { page, n } = getPage(data.index)
    setBit(page, n)
  }

  const roots = await getRoots(head.length, getTreeNode)

  let w = core.write()
  for (const index of allBits(bitfield)) {
    const { page, n } = getPage(index)
    setBit(page, n)

    const blk = await getBlockFromFile(files.data, index, roots, getTreeNode)

    if (w.changes.length > 1024) {
      await w.flush()
      w = core.write()
    }

    w.putBlock(index, blk)
  }

  for (const [index, page] of pages) {
    w.putBitfieldPage(index, b4a.from(page.buffer, page.byteOffset, page.byteLength))
  }

  await w.flush()

  await commitCoreMigration(auth, core, version)

  if (gc) await runGC()

  async function runGC () {
    await rm(files.path)
    await rmdir(path.join(files.path, '..'))
    await rmdir(path.join(files.path, '../..'))
    await rmdir(path.join(core.store.path, 'cores'))
  }

  function getPage (index) {
    const n = index & 32767
    const p = (index - n) / 32768

    let page = pages.get(p)
    if (page) return { n, page }

    page = new Uint32Array(1024)
    pages.set(p, page)

    return { n, page }
  }

  function getTreeNode (index) {
    const read = core.read()
    const promise = read.getTreeNode(index)
    read.tryFlush()
    return promise
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

function getFiles (dir) {
  return {
    path: dir,
    oplog: path.join(dir, 'oplog'),
    data: path.join(dir, 'data'),
    tree: path.join(dir, 'tree'),
    bitfield: path.join(dir, 'bitfield')
  }
}

async function getRoots (length, getTreeNode) {
  const all = []
  for (const index of flat.fullRoots(2 * length)) {
    all.push(await getTreeNode(index))
  }
  return all
}

async function getBlockFromFile (file, index, roots, getTreeNode) {
  const size = (await getTreeNode(2 * index)).size
  const offset = await getByteOffset(2 * index, roots, getTreeNode)

  return new Promise(function (resolve) {
    readAll(file, size, offset, function (err, buf) {
      if (err) return resolve(null)
      resolve(buf)
    })
  })
}

async function getByteOffset (index, roots, getTreeNode) {
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

    while (ite.index !== index) {
      if (index < ite.index) {
        ite.leftChild()
      } else {
        offset += (await getTreeNode(ite.leftChild())).size
        ite.sibling()
      }
    }

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
        if (n & m) yield i * EMPTY_PAGE.byteLength * 8 + j * 32 + k
      }
    }
  }
}

function setBit (page, n) {
  const o = n & 31
  const b = (n - o) / 32
  const v = 1 << o

  page[b] |= v
}
