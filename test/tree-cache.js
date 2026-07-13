const test = require('brittle')
const b4a = require('b4a')
const { createCore } = require('./helpers')

test('tree cache is bypassed without a fork', async (t) => {
  const core = await createCore(t)
  const node = treeNode(0, 1)

  await putTreeNodes(core, [node])

  const get = core.treeCache.get
  let cacheReads = 0

  core.treeCache.get = function (key) {
    cacheReads++
    return get.call(this, key)
  }
  t.teardown(() => {
    core.treeCache.get = get
  })

  t.alike(await readTreeNodes(core, 1, -1), [node])
  t.is(cacheReads, 0, 'does not read from cache')
  t.is(core.store.stats.treeCache.hits, 0, 'stats show no hits')
  t.is(core.store.stats.treeCache.skips, 1, 'stats skips inc')
})

test('tree cache is used with a fork', async (t) => {
  const core = await createCore(t)
  const node = treeNode(0, 1)

  await putTreeNodes(core, [node])

  const [result1] = await readTreeNodes(core, 1, 0)
  t.is(core.store.stats.treeCache.misses, 1, 'initial miss')
  t.is(core.store.stats.treeCache.hits, 0, 'no hit')
  t.alike(result1, node, 'returns node')

  const [result2] = await readTreeNodes(core, 1, 0)
  t.alike(result2, node, '2nd read returns node')
  t.is(result1, result2, 'caches same tree node')
  t.is(core.store.stats.treeCache.hits, 1, 'stats has hit')
})

test('tree cache is bypassed for atomized reads', async (t) => {
  const core = await createCore(t)
  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(core, [first])
  t.alike(await readTreeNodes(core, 1, 0), [first], 'caches parent node')
  t.is(core.store.stats.treeCache.misses, 1, 'initial miss')

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)
  t.teardown(() => atomCore.close())

  await putTreeNodes(atomCore, [second])

  t.alike(
    await readTreeNodes(atomCore, 1, 0),
    [second],
    'reads atomized node instead of cached parent node'
  )
  t.is(core.store.stats.treeCache.skips, 1, 'atom skips')
  t.alike(await readTreeNodes(core, 1, 0), [first], 'parent cache remains unchanged')
  t.is(core.store.stats.treeCache.hits, 1, 're-reading hits')
})

test('tree cache isolates overwritten nodes by fork', async (t) => {
  const core = await createCore(t)

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(core, [first])
  t.alike(await readTreeNodes(core, 1, 0), [first], 'cached first node')
  t.is(core.store.stats.treeCache.misses, 1, 'first miss')
  t.is(core.store.stats.treeCache.hits, 0, 'no hit')

  await putTreeNodes(core, [second])
  t.alike(await readTreeNodes(core, 1, 1), [second], 'next fork reads overwritten node')
  t.is(core.store.stats.treeCache.misses, 2, 'fork causes miss')
  t.alike(await readTreeNodes(core, 1, 0), [first], 'previous fork retains cached node')
  t.is(core.store.stats.treeCache.hits, 1, 'old fork hits')
})

test('tree cache isolates deleted nodes by fork', async (t) => {
  const core = await createCore(t)

  const nodes = [treeNode(0, 1), treeNode(1, 2), treeNode(2, 3), treeNode(3, 4)]

  await putTreeNodes(core, nodes)
  t.alike(await readTreeNodes(core, 4, 0), nodes, 'cached all nodes')

  const tx = core.write()
  tx.deleteTreeNodeRange(1, 3)
  await tx.flush()

  t.alike(
    await readTreeNodes(core, 4, 1),
    [nodes[0], null, null, nodes[3]],
    'next fork does not return deleted nodes'
  )
  t.alike(await readTreeNodes(core, 4, 0), nodes, 'previous fork retains cached nodes')
})

test('named session overwrite is isolated by fork', async (t) => {
  const core = await createCore(t)
  const session = await core.createSession('session', null)
  t.teardown(() => session.close())

  const node = treeNode(0, 1)

  await putTreeNodes(session, [node])
  t.alike(await readTreeNodes(session, 1, 0), [node], 'cached named-session node')

  const replacement = await core.createSession('session', null)
  t.teardown(() => replacement.close())

  t.alike(await readTreeNodes(replacement, 1, 1), [null], 'replacement observes overwritten state')
  t.alike(await readTreeNodes(session, 1, 1), [null], 'existing session observes overwritten state')
  t.alike(await readTreeNodes(session, 1, 0), [node], 'previous fork retains cached node')
})

test('parent reads atom flush under the next fork', async (t) => {
  const core = await createCore(t)

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(core, [first])
  t.alike(await readTreeNodes(core, 1, 0), [first], 'cached parent node')

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  await putTreeNodes(atomCore, [second])
  t.alike(await readTreeNodes(core, 1, 0), [first], 'parent unchanged before atom flush')
  t.alike(await readTreeNodes(atomCore, 1, 1), [second], 'atom sees overwritten node')

  await atom.flush()
  t.alike(await readTreeNodes(core, 1, 1), [second], 'parent sees atom result')
  t.alike(await readTreeNodes(core, 1, 0), [first], 'previous fork retains cached node')
})

test('atomized siblings observe shared view writes', async (t) => {
  const core = await createCore(t)
  const atom = core.createAtom()
  const firstSession = core.atomize(atom)
  const secondSession = core.atomize(atom)

  t.teardown(() => firstSession.close())
  t.teardown(() => secondSession.close())

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(firstSession, [first])
  t.alike(await readTreeNodes(secondSession, 1, 0), [first], 'sibling reads first node')

  await putTreeNodes(firstSession, [second])
  t.alike(await readTreeNodes(secondSession, 1, 1), [second], 'sibling observes overwritten node')
})

async function readTreeNodes(core, nr, fork) {
  const rx = core.read(fork)

  const proms = []
  for (let i = 0; i < nr; i++) proms.push(rx.getTreeNode(i))
  rx.tryFlush()
  return await Promise.all(proms)
}

async function putTreeNodes(core, nodes) {
  const tx = core.write()
  for (const node of nodes) tx.putTreeNode(node)
  await tx.flush()
}

function treeNode(index, value) {
  return {
    index,
    size: 1,
    hash: b4a.alloc(32, value)
  }
}
