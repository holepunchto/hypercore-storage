const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

test('make storage and core', async function (t) {
  const s = await create(t)

  t.is(await s.has(b4a.alloc(32)), false)
  t.is(await s.resume(b4a.alloc(32)), null)

  const c = await s.create({ key: b4a.alloc(32), discoveryKey: b4a.alloc(32) })

  t.is(await s.has(b4a.alloc(32)), true)

  await c.close()

  t.is(await s.has(b4a.alloc(32)), true)

  const r = await s.resume(b4a.alloc(32))

  t.ok(!!r)

  await r.close()
  await s.close()
})

test('make many in parallel', async function (t) {
  const s = await create(t)

  const all = []
  for (let i = 0; i < 50; i++) {
    const c = s.create({ key: b4a.alloc(32, i), discoveryKey: b4a.alloc(32, i) })
    all.push(c)
  }

  const cores = await Promise.all(all)
  const ptrs = new Set()

  for (const c of cores) {
    ptrs.add(c.core.corePointer)
  }

  // all unique allocations
  t.is(ptrs.size, cores.length)

  for (const c of cores) await c.close()

  await s.close()
})

test('first core created is the default core', async function (t) {
  const s = await create(t)

  t.is(await s.getDefaultDiscoveryKey(), null)
  const c = await s.create({ key: b4a.alloc(32), discoveryKey: b4a.alloc(32) })

  t.alike(await s.getDefaultDiscoveryKey(), b4a.alloc(32))

  const c1 = await s.create({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })

  t.alike(await s.getDefaultDiscoveryKey(), b4a.alloc(32))

  await c.close()
  await c1.close()
  await s.close()
})

test('first core created is the default core', async function (t) {
  const s = await create(t)

  t.is(await s.getDefaultDiscoveryKey(), null)
  const c = await s.create({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 2) })

  t.alike(await s.getDefaultDiscoveryKey(), b4a.alloc(32, 2))

  const auth = await s.getAuth(b4a.alloc(32, 2))

  t.alike(auth, {
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 2),
    manifest: null,
    keyPair: null,
    encryptionKey: null
  })

  await c.close()
  await s.close()
})
