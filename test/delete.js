const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const { create, writeBlocks, readBlocks, toArray } = require('./helpers')

test('deleteCore - removes the core, its blocks and its alias', async function (t) {
  const s = await create(t)
  t.teardown(() => s.close())

  const namespace = b4a.alloc(32, 1)
  const alias = { name: 'bee', namespace }
  const discoveryKey = crypto.randomBytes(32)

  const core = await s.createCore({ key: crypto.randomBytes(32), discoveryKey, alias })
  await writeBlocks(core, 5)

  t.alike(await s.getAlias(alias), discoveryKey, 'alias resolves before delete')

  await s.deleteCore(core.core)
  await core.close()

  t.is(await s.hasCore(discoveryKey), false, 'core record is gone')
  t.is(await s.getAlias(alias), null, 'alias is gone')
  t.alike(await toArray(s.createDiscoveryKeyStream(namespace)), [], 'namespace listing is empty')
  t.alike(await toArray(s.createDiscoveryKeyStream()), [], 'store listing is empty')
})

test('deleteCore - a deleted alias can be minted again', async function (t) {
  const s = await create(t)
  t.teardown(() => s.close())

  const namespace = b4a.alloc(32, 2)
  const alias = { name: 'bee', namespace }

  const first = await s.createCore({
    key: crypto.randomBytes(32),
    discoveryKey: crypto.randomBytes(32),
    alias
  })
  await writeBlocks(first, 3)
  await s.deleteCore(first.core)
  await first.close()

  const discoveryKey = crypto.randomBytes(32)
  const second = await s.createCore({ key: crypto.randomBytes(32), discoveryKey, alias })
  await writeBlocks(second, 2)

  t.alike(await s.getAlias(alias), discoveryKey, 'alias points at the new core')
  t.alike(await toArray(s.createDiscoveryKeyStream(namespace)), [discoveryKey])
  t.is((await readBlocks(second, 2)).length, 2)

  await second.close()
})

test('deleteCore - other aliases in the namespace survive', async function (t) {
  const s = await create(t)
  t.teardown(() => s.close())

  const namespace = b4a.alloc(32, 3)
  const cores = []

  for (let i = 0; i < 3; i++) {
    const discoveryKey = crypto.randomBytes(32)
    const core = await s.createCore({
      key: crypto.randomBytes(32),
      discoveryKey,
      alias: { name: 'core-' + i, namespace }
    })
    cores.push({ core, discoveryKey })
  }

  await s.deleteCore(cores[1].core.core)
  for (const { core } of cores) await core.close()

  t.is(await s.getAlias({ name: 'core-1', namespace }), null)
  t.alike(await s.getAlias({ name: 'core-0', namespace }), cores[0].discoveryKey)
  t.alike(await s.getAlias({ name: 'core-2', namespace }), cores[2].discoveryKey)
  t.is((await toArray(s.createDiscoveryKeyStream(namespace))).length, 2)
})

test('deleteAlias - drops the alias and leaves the core alone', async function (t) {
  const s = await create(t)
  const namespace = b4a.alloc(32, 7)
  const alias = { name: 'bee', namespace }
  const discoveryKey = b4a.alloc(32, 1)

  const core = await s.createCore({ key: crypto.randomBytes(32), discoveryKey, alias })
  await writeBlocks(core, 2)
  await core.close()

  t.is(
    await s.deleteAlias(alias, b4a.alloc(32, 9)),
    false,
    'a mismatching discovery key is refused'
  )
  t.alike(await s.getAlias(alias), discoveryKey, 'alias untouched')

  t.is(await s.deleteAlias(alias, discoveryKey), true)

  t.is(await s.getAlias(alias), null, 'alias is gone')
  t.is(await s.hasCore(discoveryKey), true, 'core record survives')
  t.alike(await toArray(s.createDiscoveryKeyStream()), [discoveryKey], 'core still listed')

  t.is(await s.deleteAlias(alias, discoveryKey), false, 'deleting a missing alias is a noop')

  await s.close()
})

test('deleteCore - compact reclaims the deleted ranges', async function (t) {
  // over minBlobSize, so the values go to blob files shared with the other core
  const blocks = { pre: 'x'.repeat(8192) }

  const expected = []
  for (let i = 0; i < 5; i++) expected.push(b4a.from(blocks.pre + 'block' + i))

  for (const compact of [false, true]) {
    const s = await create(t)

    const discoveryKey = crypto.randomBytes(32)
    const otherKey = crypto.randomBytes(32)

    const core = await s.createCore({ key: crypto.randomBytes(32), discoveryKey })
    const other = await s.createCore({ key: crypto.randomBytes(32), discoveryKey: otherKey })

    await writeBlocks(core, 50, blocks)
    await writeBlocks(other, 5, blocks)

    await s.db.flush()
    const before = await diskUsage(s)

    await core.close()
    await s.deleteCore(core.core, { compact })

    const after = await diskUsage(s)

    if (compact) t.ok(after < before / 2, 'the deleted blocks are off disk')
    else t.ok(after > before * 0.9, 'the deleted blocks are still on disk')

    t.is(await s.hasCore(discoveryKey), false, 'core record is gone')
    t.is(await s.hasCore(otherKey), true, 'other core survives')
    t.alike(await readBlocks(other, 5), expected, 'other core still readable')

    await other.close()
    await s.close()
  }
})

async function diskUsage(s) {
  const sst = await s.db.getProperty('rocksdb.total-sst-files-size')
  const blobs = await s.db.getProperty('rocksdb.total-blob-file-size')

  return Number(sst) + Number(blobs)
}

test('compactBlockRange - reclaims the cleared blocks only', async function (t) {
  // under minBlobSize, so the values are stored inline in the table files
  const blocks = { pre: 'x'.repeat(1024) }

  const s = await create(t)

  const core = await s.createCore({
    key: crypto.randomBytes(32),
    discoveryKey: crypto.randomBytes(32)
  })

  await writeBlocks(core, 400, blocks)

  await s.db.flush()
  const before = await diskUsage(s)

  const tx = core.write()
  tx.deleteBlockRange(0, 200)
  await tx.flush()

  await s.db.flush()
  t.ok((await diskUsage(s)) > before * 0.9, 'deleting alone leaves the blocks on disk')

  await core.compactBlockRange(0, 200)
  t.ok((await diskUsage(s)) < before * 0.75, 'compacting the range gives the space back')

  const expected = []
  for (let i = 200; i < 205; i++) expected.push(b4a.from(blocks.pre + 'block' + i))

  const rx = core.read()
  const kept = [200, 201, 202, 203, 204].map((i) => rx.getBlock(i))
  rx.tryFlush()

  t.alike(await Promise.all(kept), expected, 'the blocks outside the range survive')

  await core.close()
  await s.close()
})
