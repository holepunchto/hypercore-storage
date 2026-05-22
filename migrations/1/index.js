const c = require('compact-encoding')
const m = require('./messages.js')
const schema = require('../../encoding/spec/hyperschema')
const { core: coreKey } = require('../../lib/keys.js')
const View = require('../../lib/view.js')

const CORE_AUTH = schema.getEncoding('@core/auth')

const { CorestoreRX, CorestoreTX } = require('../../lib/tx.js')

async function core (core, { version, dryRun = false } = {}) {
  if (dryRun) return // dryRun mode not supported atm

  const rx = core.db.read({ autoDestroy: true })
  const promises = [
    rx.get(coreKey.auth(core.core.corePointer)),
    rx.get(coreKey.head(core.core.corePointer))
  ]

  rx.tryFlush()

  const [storedAuth, storedHead] = await Promise.all(promises)
  if (!storedAuth) return

  const auth = c.decode(CORE_AUTH, storedAuth)
  console.log({ auth })
  if (storedHead === null) {
    await commitCoreMigration(auth, core, version)
    return // no data
  }

  const head = c.decode(m.head, storedHead)

  const migratedHead = {
    fork: head.fork,
    length: head.length,
    rootHash: head.rootHash,
    signature: head.signature,
    timestamp: 0
  }

  const tx = core.write()
  tx.setHead(migratedHead)
  await tx.flush()

  await commitCoreMigration(auth, core, version)
}

module.exports = { core }

async function commitCoreMigration (auth, core, version) {
  const view = new View()
  const rx = new CorestoreRX(core.db, view)

  const storeCorePromise = rx.getCore(auth.discoveryKey)
  rx.tryFlush()

  const storeCore = await storeCorePromise

  storeCore.version = version

  const tx = new CorestoreTX(view)
  tx.putCore(auth.discoveryKey, storeCore)
  tx.apply()

  await View.flush(view.changes, core.db)
}
