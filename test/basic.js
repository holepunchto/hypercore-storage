const test = require('brittle')
const b4a = require('b4a')
const tmp = require('test-tmp')
const Storage = require('../')
const { create, writeBlocks, readBlocks } = require('./helpers')
const { core: keys } = require('../lib/keys.js')

test('make storage and core', async function (t) {
  const s = await create(t)

  t.is(await s.hasCore(b4a.alloc(32)), false)
  t.is(await s.resumeCore(b4a.alloc(32)), null)

  const c = await s.createCore({ key: b4a.alloc(32), discoveryKey: b4a.alloc(32) })

  t.is(await s.hasCore(b4a.alloc(32)), true)

  await c.close()

  t.is(await s.hasCore(b4a.alloc(32)), true)

  const r = await s.resumeCore(b4a.alloc(32))

  t.ok(!!r)

  await r.close()
  await s.close()
})

test('onresume hook fires for existing and missing cores', async function (t) {
  const resumed = []
  const s = new Storage(await tmp(t), { onresume: (discoveryKey) => resumed.push(discoveryKey) })

  t.is(await s.resumeCore(b4a.alloc(32, 1)), null)
  t.alike(resumed, [b4a.alloc(32, 1)])

  const c = await s.createCore({ key: b4a.alloc(32), discoveryKey: b4a.alloc(32) })
  await c.close()

  const r = await s.resumeCore(b4a.alloc(32))
  t.ok(!!r)
  t.alike(resumed, [b4a.alloc(32, 1), b4a.alloc(32)])

  await r.close()
  await s.close()
})

test('make many in parallel', async function (t) {
  const s = await create(t)

  const all = []
  for (let i = 0; i < 50; i++) {
    const c = s.createCore({
      key: b4a.alloc(32, i),
      discoveryKey: b4a.alloc(32, i)
    })
    all.push(c)
  }

  const cores = await Promise.all(all)
  const ptrs = new Set()

  for (const c of cores) {
    ptrs.add(c.core.corePointer)
  }

  // all unique allocations
  t.is(ptrs.size, cores.length)

  await s.setRecovering()

  for (const c of cores) await c.close()

  await s.close()
})

test('first core created is the default core', async function (t) {
  const s = await create(t)

  t.is(await s.getDefaultDiscoveryKey(), null)
  const c = await s.createCore({ key: b4a.alloc(32), discoveryKey: b4a.alloc(32) })

  t.alike(await s.getDefaultDiscoveryKey(), b4a.alloc(32))

  const c1 = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 1)
  })

  t.alike(await s.getDefaultDiscoveryKey(), b4a.alloc(32))

  await c.close()
  await c1.close()
  await s.close()
})

test('first core created is the default core', async function (t) {
  const s = await create(t)

  t.is(await s.getDefaultDiscoveryKey(), null)
  const c = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 2)
  })

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
  const c = await s.createCore({
    key: b4a.alloc(32, 1),
    discoveryKey: b4a.alloc(32, 2)
  })

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
    const c = s.createCore({
      key: b4a.alloc(32, i),
      discoveryKey: b4a.alloc(32, i)
    })
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
    signers: [
      {
        signature: 'ed25519',
        namespace: b4a.alloc(32, 0),
        publicKey: b4a.alloc(32, 0)
      }
    ],
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

test('can get info from store efficiently', async function (t) {
  const s = await create(t)
  const dkeys = []

  for (let i = 0; i < 5; i++) {
    const discoveryKey = b4a.alloc(32, i)
    const c = await s.createCore({
      key: b4a.alloc(32, i),
      discoveryKey
    })

    dkeys.push(discoveryKey)

    const w = c.write()

    w.setHead({
      fork: 0,
      length: i,
      rootHash: b4a.alloc(32),
      signature: null
    })

    await w.flush()
  }

  {
    const heads = await s.getInfos(dkeys)
    t.alike(
      heads.map((x) => x.head.length),
      [0, 1, 2, 3, 4]
    )
  }

  {
    const heads = await s.getInfos(dkeys.concat(b4a.alloc(32, 'nope')))
    t.alike(
      heads.map((x) => x === null),
      [false, false, false, false, false, true]
    )
  }

  {
    const head = await s.getInfo(dkeys[2])
    t.alike(head, {
      discoveryKey: dkeys[2],
      core: head.core, // dont wanna over assert this one
      auth: {
        key: b4a.alloc(32, 2),
        discoveryKey: b4a.alloc(32, 2),
        manifest: null,
        keyPair: null,
        encryptionKey: null
      },
      head: { fork: 0, length: 2, rootHash: b4a.alloc(32, 0), signature: null, timestamp: 0 },
      hints: null
    })
  }

  await s.close()
})

test('deleteCore removes all of its data and leaves other cores alone', async function (t) {
  const s = await create(t)

  const a = await s.createCore({ key: b4a.alloc(32, 1), discoveryKey: b4a.alloc(32, 1) })
  const b = await s.createCore({ key: b4a.alloc(32, 2), discoveryKey: b4a.alloc(32, 2) })

  await writeBlocks(a, 3)
  await writeBlocks(b, 3)

  const named = await a.createSession('named', null)
  await writeBlocks(named, 2, { pre: 'named-' })

  const ptr = a.core
  const namedDataPointer = named.core.dataPointer

  await named.close()
  await a.close()

  await s.deleteCore(ptr)

  t.is(await s.hasCore(b4a.alloc(32, 1)), false)
  t.is(await count(s, keys.core(ptr.corePointer), keys.core(ptr.corePointer + 1)), 0)
  t.is(await count(s, keys.data(ptr.dataPointer), keys.data(ptr.dataPointer + 1)), 0)
  t.is(await count(s, keys.data(namedDataPointer), keys.data(namedDataPointer + 1)), 0)

  t.alike(await readBlocks(b, 3), [b4a.from('block0'), b4a.from('block1'), b4a.from('block2')])

  await b.close()
  await s.close()
})

async function count(s, gte, lt) {
  let n = 0
  for await (const _ of s.db.iterator({ gte, lt })) n++
  return n
}
