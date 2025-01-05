// This file is autogenerated by the hyperschema compiler
// Schema Version: 1
/* eslint-disable camelcase */
/* eslint-disable quotes */

const VERSION = 1
const { c } = require('hyperschema/runtime')

// eslint-disable-next-line no-unused-vars
let version = VERSION

// @corestore/head
const encoding0 = {
  preencode (state, m) {
    const flags =
      (m.total ? 1 : 0) |
      (m.next ? 2 : 0) |
      (m.seed ? 4 : 0)

    c.uint.preencode(state, m.version)
    c.uint.preencode(state, flags)

    if (m.total) c.uint.preencode(state, m.total)
    if (m.next) c.uint.preencode(state, m.next)
    if (m.seed) c.fixed32.preencode(state, m.seed)
  },
  encode (state, m) {
    const flags =
      (m.total ? 1 : 0) |
      (m.next ? 2 : 0) |
      (m.seed ? 4 : 0)

    c.uint.encode(state, m.version)
    c.uint.encode(state, flags)

    if (m.total) c.uint.encode(state, m.total)
    if (m.next) c.uint.encode(state, m.next)
    if (m.seed) c.fixed32.encode(state, m.seed)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      total: (flags & 1) !== 0 ? c.uint.decode(state) : 0,
      next: (flags & 2) !== 0 ? c.uint.decode(state) : 0,
      seed: (flags & 4) !== 0 ? c.fixed32.decode(state) : null
    }
  }
}

// @core/tree-node
const encoding1 = {
  preencode (state, m) {
    c.uint.preencode(state, m.index)
    c.uint.preencode(state, m.size)
    c.fixed32.preencode(state, m.hash)
  },
  encode (state, m) {
    c.uint.encode(state, m.index)
    c.uint.encode(state, m.size)
    c.fixed32.encode(state, m.hash)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)
    const r2 = c.fixed32.decode(state)

    return {
      index: r0,
      size: r1,
      hash: r2
    }
  }
}

// @core/dependencies
const encoding2 = c.array({
  preencode (state, m) {
    c.uint.preencode(state, m.dataPointer)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, m.dataPointer)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)

    return {
      dataPointer: r0,
      length: r1
    }
  }
})

function setVersion (v) {
  version = v
}

function encode (name, value, v = VERSION) {
  version = v
  return c.encode(getEncoding(name), value)
}

function decode (name, buffer, v = VERSION) {
  version = v
  return c.decode(getEncoding(name), buffer)
}

function getEncoding (name) {
  switch (name) {
    case '@corestore/head': return encoding0
    case '@core/tree-node': return encoding1
    case '@core/dependencies': return encoding2
    default: throw new Error('Encoder not found ' + name)
  }
}

function resolveStruct (name, v = VERSION) {
  const enc = getEncoding(name)
  return {
    preencode (state, m) {
      version = v
      enc.preencode(state, m)
    },
    encode (state, m) {
      version = v
      enc.encode(state, m)
    },
    decode (state) {
      version = v
      return enc.decode(state)
    }
  }
}

module.exports = { resolveStruct, getEncoding, encode, decode, setVersion, version }
