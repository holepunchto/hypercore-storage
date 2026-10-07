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
