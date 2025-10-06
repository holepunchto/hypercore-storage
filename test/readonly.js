const test = require('brittle')
const b4a = require('b4a')
const tmp = require('test-tmp')
const Storage = require('../')

test('make storage and core', async function (t) {
  const dir = await tmp(t)
  const w = new Storage(dir)

  t.is(await w.has(b4a.alloc(32)), false)
  t.is(await w.resumeCore(b4a.alloc(32)), null)

  await w.db.suspend()
  await w.db.resume()

  const ro = new Storage(dir, { readOnly: true })

  t.is(w.path, ro.path)

  await ro.suspend()
  t.ok(ro.db._state._suspending)
  await ro.resume()
  t.absent(ro.db._state._suspending)

  await w.close()
  await ro.close()

  t.pass()
})
