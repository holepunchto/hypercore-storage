const test = require('brittle')
const path = require('path')
const { access } = require('fs/promises')
const Corestore6_18_4 = require('corestore-6.18.4')
const CorestoreStorage = require('../index.js')

test('migration - basic from corestore@6.18.4', async (t) => {
  const dir = await t.tmp()
  let coreKey
  let coreDKey
  const primaryKeyFile = path.join(dir, 'primary-key')
  // Setup old core
  {
    const store = new Corestore6_18_4(dir)
    const a = store.get({ name: 'a', valueEncoding: 'utf8' })
    await a.ready()
    await a.append('beep')
    coreKey = a.key // setup for opening later
    coreDKey = a.discoveryKey // setup for opening later
    t.is(await a.get(0), 'beep')
    t.ok(await fileExists(primaryKeyFile), 'primary key file exists from old version')
  }

  // New storage
  const storage = new CorestoreStorage(dir)
  t.is(storage.version, 0, 'version 0')

  t.ok(await fileExists(primaryKeyFile), 'primary key file exists')

  t.ok(await storage.hasCore(coreDKey), 'finds old core & triggers migration')
  t.is(storage.version, 1, 'version 1')
  t.absent(await fileExists(primaryKeyFile), "primary key was gc'ed")
})

async function fileExists(path) {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}
