import Rocks from './index.js'

const r = new Rocks('/tmp/rocks')

const w = r.createWriteBatch()

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

for await (const node of r.createTreeNodeStream()) {
  console.log('tree node', node)
}

console.log('reversing')

for await (const node of r.createTreeNodeStream({ reverse: true })) {
  console.log('tree node', node)
}

console.log('peek')

for await (const node of r.createTreeNodeStream({ reverse: true, limit: 1 })) {
  console.log('peak last tree node', node)
}
