# hypercore-storage

The storage engine for Hypercore. Built on RocksDB.

```
npm install hypercore-storage
```

## API

The following API is what Hypercore 11 binds to to do I/O.

```js
const Storage = require('hypercore-storage')
```

#### `store = new Storage(dbOrPath)`

Make a new storage engine.

#### `core = await store.create({ key, discoveyKey, manifest?, keyPair?, encryptionKey?, userData? })`

Create a new core, returns a storage instance for that core.

#### `core = await store.resume(discoveryKey)`

Resume a previously make core. If it doesn't exist it returns `null`.

#### `atom = store.createAtom()`

Primitive for making atomic batches across ops. See below for `core.atomize` on how to use it.
When you wanna flush your changes to the underlying storage, use `await atom.flush()`.

Internally to "listen" for when that happens you can add an sync hook with `atom.onflush(fn)`

#### `bool = await store.has(discoveryKey)`

Check if a core exists.

#### `stream = store.createCoreStream()`

List all cores. Stream data looks like this `{ discoveryKey, core }` where core contains the core header.

#### `await store.close()`

Close the storage instance.

#### `rx = core.read()`

Make a read batch on a core storage.

**NOTE:** a read batch DOES NOT flush until you call `rx.tryFlush()`.

#### `await rx.getAuth()`

Returns the auth data around a core.

#### `await rx.getHead()`

Returns the head of the merkle tree.

#### `await rx.getSessions()`

Returns an array of all named sessions.

#### `await rx.getDependency()`

Returns the core this has a dependency on.

#### `await rx.getHints()`

Returns the various storage/replication hints.

#### `await rx.getBlock(index)`

Returns a block stored.

#### `await rx.getTreeNode(index)`

Returns a tree node stored.

#### `await rx.getBitfieldPage(index)`

Return a bitfield page.

#### `await rx.getUserData(key)`

Return a user stored buffer.

#### `rx.tryFlush()`

Flushes the read batch, non of the above promises will resolve until you call this.

#### `tx = core.write()`

Make a write batch on a core storage.

**NOTE:** all the apis below are sync as they just buffer mutations until you flush them.

#### `tx.setAuth(auth)`

Set the auth data around a core.

#### `tx.setHead(auth)`

Set the head of the merkle tree.

#### `tx.setSessions(sessions)`

Set an array of all named sessions.

#### `tx.setDependency(dep)`

Set the core this has a dependency on.

#### `tx.setHints(hints)`

Set the various storage/replication hints.

#### `tx.putBlock(index, buffer)`

Put a block at a specific index.

#### `tx.deleteBlock(index)`

Delete a block at a specific index.

#### `tx.deleteBlockRange(start, index)`

Delete blocks between two indexes.

#### `tx.putTreeNode(node)`

Put a tree node (at its described index).

#### `tx.deleteTreeNode(index)`

Delete a tree node at a specific index.

#### `tx.deleteTreeNodeRange(start, index)`

Delete blocks between two tree indexes.

#### `tx.putBitfieldPage(index, page)`

Put a bitfield page at its described index.

#### `tx.deleteBitfieldPage(index)`

Delete a bitfield page.

#### `tx.deleteBitfieldPageRange(start, end)`

Delete bitfield pages between two indexes.

#### `tx.putUserData(key, value)`

Put a user provided buffer at a user provided key.

#### `tx.deleteUserData(key)`

Delete a user provided key.

#### `await tx.flush()`

Flushes the write batch.

#### `stream = core.createBlockStream(opts)`

Create a stream of all blocks.

#### `stream = core.createTreeNodeStream(opts)`

Create a stream of all tree nodes.

#### `stream = core.createBitfieldStream(opts)`

Create a stream of all bitfield pages.

#### `stream = core.createUserDataStream(opts)`

Create a stream of all user data.

#### `await core.close()`

Close the core storage engine.

#### `atom = core.createAtom()`

Same as `store.createAtom()` but here again for conveinience.

#### `core = core.atomize(atom)`

Atomize a core. Allows you to build up cross core atomic batches and operations.
An atomized core will not flush its changes until you call `atom.flush()`, but you can still read your writes.

#### `core = core.createSession(name, head)`

Create a named session on top of a core. A named session points back to the previous storage,
but is otherwise independent and stored on disk, like a branch in git if you will.

#### `core.dependencies`

Array containing the full list of dependencies for this core (ie tree of named sessions).

## License

Apache-2.0
