const b4a = require('b4a')
const tmp = require('test-tmp')
const Storage = require('../../')

module.exports = { createCore, create, toArray }

async function createCore (t) {
  const s = await create(t)
  const core = await s.create({ key: b4a.alloc(32), discoveryKey: b4a.alloc(32) })

  t.teardown(async function () {
    await core.close()
    await s.close()
  })

  return core
}

async function create (t) {
  return new Storage(await tmp(t))
}

async function toArray (stream) {
  const all = []
  for await (const data of stream) all.push(data)
  return all
}
