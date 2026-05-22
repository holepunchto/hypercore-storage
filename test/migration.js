const test = require('brittle')
const b4a = require('b4a')
const path = require('path')
const os = require('os')
const HypercoreStorage = require('../index.js')
const { CorestoreRX } = require('../lib/tx.js')
const View = require('../lib/view.js')

const skip = os.platform() !== 'darwin' // fixture was generated on darwin/macos

test('migrate v2 -> v3 - core migration (macos)', { skip }, async (t) => {
  const dir = path.join(__dirname, './fixtures/2.9.0-darwin')
  const storage = new HypercoreStorage(dir, { allowBackup: true })
  const EMPTY = new View()

  const discoveryKey = b4a.alloc(32)

  {
    // Core entry before
    // v2.9.0 fixture was generated with version = 1
    const rx2 = new CorestoreRX(storage.db, EMPTY)
    const corePromise = rx2.getCore(discoveryKey)

    rx2.tryFlush()
    const core = await corePromise
    t.is(core.version, 1, 'core entry starts version 1')
  }

  t.is(await storage.hasCore(discoveryKey), true)
  const c = await storage.resumeCore(b4a.alloc(32))
  t.ok(c, 'got core')

  {
    const rx = c.read()

    // v2.9.0 fixture was generated with head for dkey b4a.alloc(32) with:
    // {
    //   fork: 3,
    //   length: 2,
    //   rootHash: b4a.alloc(32),
    //   signature: null
    // }
    // So no timestamp

    const headPromise = rx.getHead()
    rx.tryFlush()

    const head = await headPromise
    t.is(head.timestamp, 0, 'timestamp default set')
    t.is(head.fork, 3, 'got fork')
    t.is(head.length, 2, 'got length')
    t.alike(head.rootHash, b4a.alloc(32), 'got rootHash')
    t.is(head.signature, null, 'got signature')

    // Core entry
    // v2.9.0 fixture was generated with version = 1
    const rx2 = new CorestoreRX(storage.db, EMPTY)
    const corePromise = rx2.getCore(discoveryKey)

    rx2.tryFlush()
    const core = await corePromise
    t.is(core.version, 2, 'core entry upgraded to version 2')
  }

  await c.close()
  await storage.close()
})
