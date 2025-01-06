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
  fields: [{
    name: 'signature',
    type: 'uint',
    required: true
  }, {
    name: 'namespace',
    type: 'fixed32',
    required: true
  }, {
    name: 'publicKey',
    type: 'fixed32',
    required: true
  }]
})

core.register({
  name: 'prologue',
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
    type: 'uint',
    required: true
  }, {
    name: 'quorum',
    type: 'uint',
    required: true
  }, {
    name: 'allowPatch',
    type: 'bool'
  }, {
    name: 'prologue',
    type: '@core/prologue'
  }, {
    name: 'signers',
    array: true,
    type: '@core/signer'
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
