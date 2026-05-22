const test = require('brittle')
const b4a = require('b4a')

// 2.9.0
const HypercoreStorage2_9_0 = require('hypercore-storage-2.9.0')
const { CorestoreRX: CorestoreRX2_9_0 } = require('hypercore-storage-2.9.0/lib/tx.js')
const View2_9_0 = require('hypercore-storage-2.9.0/lib/view.js')

// Current
const HypercoreStorage = require('../index.js')
const { CorestoreRX } = require('../lib/tx.js')
const View = require('../lib/view.js')

test('migrate v2 -> v3 - core head', async (t) => {
  const dir = await t.tmp()
  const storagev2 = new HypercoreStorage2_9_0(dir)

  const key = b4a.alloc(32)
  const discoveryKey = b4a.alloc(32)

  t.comment('v2 - core setup')
  const cV2 = await storagev2.createCore({ key, discoveryKey })

  {
    const EMPTY = new View2_9_0()
    const rx = new CorestoreRX2_9_0(storagev2.db, EMPTY)
    const corePromise = rx.getCore(discoveryKey)

    rx.tryFlush()
    const core = await corePromise
    t.is(core.version, 1)
  }

  {
    const w = cV2.write()

    w.setHead({
      fork: 3,
      length: 2,
      rootHash: b4a.alloc(32),
      signature: null
    })

    await w.flush()
  }

  await cV2.close()
  await storagev2.close()

  t.comment('v3 - load')
  const storage = new HypercoreStorage(dir)

  t.is(await storage.hasCore(discoveryKey), true)
  const c = await storage.resumeCore(b4a.alloc(32))
  t.ok(c, 'got core')

  {
    const rx = c.read()

    const headPromise = rx.getHead()
    rx.tryFlush()

    const head = await headPromise
    t.is(head.timestamp, 0, 'timestamp default set')
    t.is(head.fork, 3, 'got fork')
    t.is(head.length, 2, 'got length')
    t.alike(head.rootHash, b4a.alloc(32), 'got rootHash')
    t.is(head.signature, null, 'got signature')

    // Core entry
    const EMPTY = new View()
    const rx2 = new CorestoreRX(storage.db, EMPTY)
    const corePromise = rx2.getCore(discoveryKey)

    rx2.tryFlush()
    const core = await corePromise
    t.is(core.version, 1, 'core entry still version 1 head')
  }

  await c.close()
  await storage.close()
})
