const Hyperschema = require('hyperschema')

const SPEC = './spec/hyperschema'

const schema = Hyperschema.from(SPEC)
const corestore = schema.namespace('corestore')

corestore.register({
  name: 'head',
  fields: [{
    name: 'version',
    type: 'uint',
    required: true
  }, {
    name: 'total',
    type: 'uint'
  }, {
    name: 'next',
    type: 'uint'
  }, {
    name: 'seed',
    type: 'fixed32'
  }]
})

const core = schema.namespace('core')

core.register({
  name: 'hashes',
  offset: 0,
  strings: true,
  enum: [
    'blake2b'
  ]
})

core.register({
  name: 'signatures',
  offset: 0,
  strings: true,
  enum: [
    'ed25519'
  ]
})

core.register({
  name: 'tree-node',
  compact: true,
  fields: [{
    name: 'index',
    type: 'uint',
    required: true
  }, {
    name: 'size',
    type: 'uint',
    required: true
  }, {
    name: 'hash',
    type: 'fixed32',
    required: true
  }]
})

core.register({
  name: 'signer',
  compact: true,
  fields: [{
    name: 'signature',
    type: '@core/signatures',
    required: true
  }, {
    name: 'namespace',
    type: 'fixed32',
    required: true
  }, {
    name: 'publicKey',
    type: 'fixed32', // should prop have been buffer but we can change when we version bump
    required: true
  }]
})

core.register({
  name: 'prologue',
  compact: true,
  fields: [{
    name: 'hash',
    type: 'fixed32',
    required: true
  }, {
    name: 'length',
    type: 'uint',
    required: true
  }]
})

core.register({
  name: 'manifest',
  flagsPosition: 1, // compat
  fields: [{
    name: 'version',
    type: 'uint',
    required: true
  }, {
    name: 'hash',
    type: '@core/hashes',
    required: true
  }, {
    name: 'quorum',
    type: 'uint',
    required: true
  }, {
    name: 'allowPatch',
    type: 'bool'
  }, {
    name: 'signers',
    array: true,
    required: true,
    type: '@core/signer'
  }, {
    name: 'prologue',
    type: '@core/prologue'
  }]
})

core.register({
  name: 'keyPair',
  compact: true,
  fields: [{
    name: 'publicKey',
    type: 'buffer',
    required: true
  }, {
    name: 'secretKey',
    type: 'buffer',
    required: true
  }]
})

core.register({
  name: 'auth',
  fields: [{
    name: 'key',
    type: 'fixed32',
    required: true
  }, {
    name: 'discoveryKey',
    type: 'fixed32',
    required: true
  }, {
    name: 'manifest',
    type: '@core/manifest'
  }, {
    name: 'signer',
    type: '@core/keyPair'
  }, {
    name: 'encryptionKey',
    type: 'buffer'
  }]
})

core.register({
  name: 'dependencies',
  compact: true,
  array: true,
  fields: [{
    name: 'dataPointer',
    type: 'uint',
    required: true
  }, {
    name: 'length',
    type: 'uint',
    required: true
  }]
})

Hyperschema.toDisk(schema, SPEC)
