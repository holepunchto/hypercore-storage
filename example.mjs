import CoreStorage from './index.js'

const s = new CoreStorage('/tmp/rocks')
const c = s.get(Buffer.alloc(32))

if (!(await c.open())) await c.create()

const w = c.createWriteBatch()

w.addTreeNode({
  index: 42,
  hash: Buffer.alloc(32),
  size: 10
})

w.addTreeNode({
  index: 43,
  hash: Buffer.alloc(32),
  size: 10
})

await w.flush()

console.log('node 42:', await c.getTreeNode(42))
console.log('node 43:', await c.getTreeNode(43))

for await (const node of c.createTreeNodeStream()) {
  console.log('tree node', node)
}

console.log('reversing')

for await (const node of c.createTreeNodeStream({ reverse: true })) {
  console.log('tree node', node)
}

console.log('peek')

for await (const node of c.createTreeNodeStream({ reverse: true, limit: 1 })) {
  console.log('peak last tree node', node)
}
