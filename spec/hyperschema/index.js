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
    c.uint.preencode(state, m.version)
    state.end++ // max flag is 4 so always one byte

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

const encoding1_enum = {
  blake2b: 'blake2b'
}

// @core/hashes enum
const encoding1 = {
  preencode (state, m) {
    state.end++ // max enum is 0 so always one byte
  },
  encode (state, m) {
    switch (m) {
      case 'blake2b':
        c.uint.encode(state, 0)
        break
      default: throw new Error('Unknown enum')
    }
  },
  decode (state) {
    switch (c.uint.decode(state)) {
      case 0: return 'blake2b'
      default: return null
    }
  }
}

const encoding2_enum = {
  ed25519: 'ed25519'
}

// @core/signatures enum
const encoding2 = {
  preencode (state, m) {
    state.end++ // max enum is 0 so always one byte
  },
  encode (state, m) {
    switch (m) {
      case 'ed25519':
        c.uint.encode(state, 0)
        break
      default: throw new Error('Unknown enum')
    }
  },
  decode (state) {
    switch (c.uint.decode(state)) {
      case 0: return 'ed25519'
      default: return null
    }
  }
}

// @core/tree-node
const encoding3 = {
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

// @core/signer
const encoding4 = {
  preencode (state, m) {
    encoding2.preencode(state, m.signature)
    c.fixed32.preencode(state, m.namespace)
    c.fixed32.preencode(state, m.publicKey)
  },
  encode (state, m) {
    encoding2.encode(state, m.signature)
    c.fixed32.encode(state, m.namespace)
    c.fixed32.encode(state, m.publicKey)
  },
  decode (state) {
    const r0 = encoding2.decode(state)
    const r1 = c.fixed32.decode(state)
    const r2 = c.fixed32.decode(state)

    return {
      signature: r0,
      namespace: r1,
      publicKey: r2
    }
  }
}

// @core/prologue
const encoding5 = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.hash)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.hash)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    const r0 = c.fixed32.decode(state)
    const r1 = c.uint.decode(state)

    return {
      hash: r0,
      length: r1
    }
  }
}

// @core/manifest.signers
const encoding6_4 = c.array(encoding4)

// @core/manifest
const encoding6 = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    state.end++ // max flag is 2 so always one byte
    encoding1.preencode(state, m.hash)
    c.uint.preencode(state, m.quorum)
    encoding6_4.preencode(state, m.signers)

    if (m.prologue) encoding5.preencode(state, m.prologue)
  },
  encode (state, m) {
    const flags =
      (m.allowPatch ? 1 : 0) |
      (m.prologue ? 2 : 0)

    c.uint.encode(state, m.version)
    c.uint.encode(state, flags)
    encoding1.encode(state, m.hash)
    c.uint.encode(state, m.quorum)
    encoding6_4.encode(state, m.signers)

    if (m.prologue) encoding5.encode(state, m.prologue)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      hash: encoding1.decode(state),
      quorum: c.uint.decode(state),
      allowPatch: (flags & 1) !== 0,
      signers: encoding6_4.decode(state),
      prologue: (flags & 2) !== 0 ? encoding5.decode(state) : null
    }
  }
}

// @core/keyPair
const encoding7 = {
  preencode (state, m) {
    c.buffer.preencode(state, m.publicKey)
    c.buffer.preencode(state, m.secretKey)
  },
  encode (state, m) {
    c.buffer.encode(state, m.publicKey)
    c.buffer.encode(state, m.secretKey)
  },
  decode (state) {
    const r0 = c.buffer.decode(state)
    const r1 = c.buffer.decode(state)

    return {
      publicKey: r0,
      secretKey: r1
    }
  }
}

// @core/auth.manifest
const encoding8_2 = c.frame(encoding6)

// @core/auth
const encoding8 = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.fixed32.preencode(state, m.discoveryKey)
    state.end++ // max flag is 4 so always one byte

    if (m.manifest) encoding8_2.preencode(state, m.manifest)
    if (m.signer) encoding7.preencode(state, m.signer)
    if (m.encryptionKey) c.buffer.preencode(state, m.encryptionKey)
  },
  encode (state, m) {
    const flags =
      (m.manifest ? 1 : 0) |
      (m.signer ? 2 : 0) |
      (m.encryptionKey ? 4 : 0)

    c.fixed32.encode(state, m.key)
    c.fixed32.encode(state, m.discoveryKey)
    c.uint.encode(state, flags)

    if (m.manifest) encoding8_2.encode(state, m.manifest)
    if (m.signer) encoding7.encode(state, m.signer)
    if (m.encryptionKey) c.buffer.encode(state, m.encryptionKey)
  },
  decode (state) {
    const r0 = c.fixed32.decode(state)
    const r1 = c.fixed32.decode(state)
    const flags = c.uint.decode(state)

    return {
      key: r0,
      discoveryKey: r1,
      manifest: (flags & 1) !== 0 ? encoding8_2.decode(state) : null,
      signer: (flags & 2) !== 0 ? encoding7.decode(state) : null,
      encryptionKey: (flags & 4) !== 0 ? c.buffer.decode(state) : null
    }
  }
}

// @core/dependencies
const encoding9 = c.array({
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

function getEnum (name) {
  switch (name) {
    case '@core/hashes': return encoding1_enum
    case '@core/signatures': return encoding2_enum
    default: throw new Error('Enum not found ' + name)
  }
}

function getEncoding (name) {
  switch (name) {
    case '@corestore/head': return encoding0
    case '@core/hashes': return encoding1
    case '@core/signatures': return encoding2
    case '@core/tree-node': return encoding3
    case '@core/signer': return encoding4
    case '@core/prologue': return encoding5
    case '@core/manifest': return encoding6
    case '@core/keyPair': return encoding7
    case '@core/auth': return encoding8
    case '@core/dependencies': return encoding9
    default: throw new Error('Encoder not found ' + name)
  }
}

function getStruct (name, v = VERSION) {
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

module.exports = { resolveStruct: getStruct, getStruct, getEnum, getEncoding, encode, decode, setVersion, version }
