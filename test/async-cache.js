const test = require('brittle')
const b4a = require('b4a')
const { createCore, readTreeNodes } = require('./helpers')

test('tree cache invalidates overwritten nodes', async (t) => {
  const core = await createCore(t)

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(core, [first])
  t.alike(await readTreeNodes(core, 1), [first], 'cached first node')

  await putTreeNodes(core, [second])
  t.alike(await readTreeNodes(core, 1), [second], 'reads overwritten node')
})

test('tree cache invalidates deleted nodes', async (t) => {
  const core = await createCore(t)

  const nodes = [treeNode(0, 1), treeNode(1, 2), treeNode(2, 3), treeNode(3, 4)]

  await putTreeNodes(core, nodes)
  t.alike(await readTreeNodes(core, 4), nodes, 'cached all nodes')

  const tx = core.write()
  tx.deleteTreeNodeRange(1, 3)
  await tx.flush()

  t.alike(
    await readTreeNodes(core, 4),
    [nodes[0], null, null, nodes[3]],
    'does not return deleted nodes'
  )
})

test('tree cache invalidation belongs to the writing transaction', async (t) => {
  const core = await createCore(t)

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(core, [first])

  const treeTx = core.write()
  treeTx.putTreeNode(second)

  const metadataTx = core.write()
  metadataTx.putUserData('key', b4a.from('value'))
  await metadataTx.flush()

  await readTreeNodes(core, 1)
  await treeTx.flush()

  t.alike(await readTreeNodes(core, 1), [second], 'tree commit invalidates its cached nodes')
})

test('overwriting a named session invalidates its existing cache (sideband writes)', async (t) => {
  const core = await createCore(t)
  const session = await core.createSession('session', null)
  t.teardown(() => session.close())

  const node = treeNode(0, 1)

  await putTreeNodes(session, [node])
  t.alike(await readTreeNodes(session, 1), [node], 'cached named-session node')

  const replacement = await core.createSession('session', null)
  t.teardown(() => replacement.close())

  t.alike(await readTreeNodes(replacement, 1), [null], 'replacement observes overwritten state')
  t.alike(await readTreeNodes(session, 1), [null], 'existing session observes overwritten state')
})

test('tree cache invalidates parent nodes after atom flush', async (t) => {
  const core = await createCore(t)

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(core, [first])
  t.alike(await readTreeNodes(core, 1), [first], 'cached parent node')

  const atom = core.createAtom()
  const atomCore = core.atomize(atom)

  await putTreeNodes(atomCore, [second])
  t.alike(await readTreeNodes(core, 1), [first], 'parent unchanged before atom flush')
  t.alike(await readTreeNodes(atomCore, 1), [second], 'atom sees overwritten node')

  await atom.flush()
  t.alike(await readTreeNodes(core, 1), [second], 'parent sees atom result')
})

test('tree cache invalidates sibling atomized sessions', async (t) => {
  const core = await createCore(t)
  const atom = core.createAtom()
  const firstSession = core.atomize(atom)
  const secondSession = core.atomize(atom)

  t.teardown(() => firstSession.close())
  t.teardown(() => secondSession.close())

  const first = treeNode(0, 1)
  const second = treeNode(0, 2)

  await putTreeNodes(firstSession, [first])
  t.alike(await readTreeNodes(secondSession, 1), [first], 'sibling caches first node')

  await putTreeNodes(firstSession, [second])
  t.alike(await readTreeNodes(secondSession, 1), [second], 'sibling observes overwritten node')
})

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
