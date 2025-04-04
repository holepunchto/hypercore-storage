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
  t.alike(await s.getAuth(b4a.alloc(32, 3)), null)

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

test('write during close', async function (t) {
  const s = await create(t)

  t.is(await s.getDefaultDiscoveryKey(), null)
  const c = await s.create({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 2) })

  const w = c.write()
  w.putUserData('test', b4a.alloc(1))
  const closing = c.close()
  try {
    await w.flush()
  } catch {
    t.pass('should fail')
  }
  await closing
  await s.close()
})

test('audit v0 cores', async function (t) {
  const s = await create(t)

  const all = []
  for (let i = 0; i < 35; i++) {
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

  const manifest = {
    version: 0,
    hash: 'blake2b',
    allowPatch: false,
    prologue: null,
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      namespace: b4a.alloc(32, 0),
      publicKey: b4a.alloc(32, 0)
    }],
    linked: []
  }

  let i = 0
  for (const c of cores) {
    const tx = c.write()
    tx.setAuth({
      key: b4a.alloc(32, 0),
      discoveryKey: b4a.alloc(32, i++),
      manifest,
      keyPair: null,
      encryptionKey: null
    })
    await tx.flush()
  }

  await s.audit()

  for (const c of cores) {
    const rx = c.read()
    const authPromise = rx.getAuth()
    rx.tryFlush()
    const { manifest } = await authPromise
    if (manifest.linked !== null) {
      t.fail('manifest.linked should be null')
      break
    }
  }

  for (const c of cores) await c.close()

  await s.close()
})
