const b4a = require('b4a')
const tmp = require('test-tmp')
const Storage = require('../../')

module.exports = {
  createCore,
  create,
  toArray,
  writeBlocks,
  readBlocks,
  readTreeNodes,
  getAuth,
  getHead,
  getDependency,
  getHints,
  getUserData,
  getBitfieldPages
}

async function createCore(t) {
  const s = await create(t)
  const core = await s.create({
    key: b4a.alloc(32),
    discoveryKey: b4a.alloc(32)
  })

  t.teardown(async function () {
    await core.close()
    await s.close()
  })

  return core
}

async function create(t) {
  return new Storage(await tmp(t))
}

async function toArray(stream) {
  const all = []
  for await (const data of stream) all.push(data)
  return all
}

async function writeBlocks(core, amount, { start = 0, pre = '' } = {}) {
  const tx = core.write()
  for (let i = start; i < amount + start; i++) {
    const content = b4a.from(`${pre}block${i}`)
    tx.putBlock(i, content)
  }
  await tx.flush()
}

async function readBlocks(core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getBlock(i))
  rx.tryFlush()
  return await Promise.all(proms)
}

async function readTreeNodes(core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getTreeNode(i))
  rx.tryFlush()
  return await Promise.all(proms)
}

async function getAuth(core) {
  const rx = core.read()
  const p = rx.getAuth()
  rx.tryFlush()
  return await p
}

async function getHead(core) {
  const rx = core.read()
  const p = rx.getHead()
  rx.tryFlush()
  return await p
}

async function getDependency(core) {
  const rx = core.read()
  const p = rx.getDependency()
  rx.tryFlush()
  return await p
}

async function getHints(core) {
  const rx = core.read()
  const p = rx.getHints()
  rx.tryFlush()
  return await p
}

async function getUserData(core, key) {
  const rx = core.read()
  const p = rx.getUserData(key)
  rx.tryFlush()
  return await p
}

async function getBitfieldPages(core, nr) {
  const rx = core.read()
  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getBitfieldPage(i))
  rx.tryFlush()
  return await Promise.all(proms)
}
