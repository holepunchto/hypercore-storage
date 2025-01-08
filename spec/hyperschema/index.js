// This file is autogenerated by the hyperschema compiler
// Schema Version: 1
/* eslint-disable camelcase */
/* eslint-disable quotes */

const VERSION = 1
const { c } = require('hyperschema/runtime')

// eslint-disable-next-line no-unused-vars
let version = VERSION

// @corestore/allocated
const encoding0 = {
  preencode (state, m) {
    c.uint.preencode(state, m.cores)
    c.uint.preencode(state, m.datas)
  },
  encode (state, m) {
    c.uint.encode(state, m.cores)
    c.uint.encode(state, m.datas)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)

    return {
      cores: r0,
      datas: r1
    }
  }
}

// @corestore/head
const encoding1 = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    state.end++ // max flag is 4 so always one byte

    if (m.allocated) encoding0.preencode(state, m.allocated)
    if (m.seed) c.fixed32.preencode(state, m.seed)
    if (m.defaultKey) c.fixed32.preencode(state, m.defaultKey)
  },
  encode (state, m) {
    const flags =
      (m.allocated ? 1 : 0) |
      (m.seed ? 2 : 0) |
      (m.defaultKey ? 4 : 0)

    c.uint.encode(state, m.version)
    c.uint.encode(state, flags)

    if (m.allocated) encoding0.encode(state, m.allocated)
    if (m.seed) c.fixed32.encode(state, m.seed)
    if (m.defaultKey) c.fixed32.encode(state, m.defaultKey)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      allocated: (flags & 1) !== 0 ? encoding0.decode(state) : null,
      seed: (flags & 2) !== 0 ? c.fixed32.decode(state) : null,
      defaultKey: (flags & 4) !== 0 ? c.fixed32.decode(state) : null
    }
  }
}

// @corestore/core
const encoding2 = {
  preencode (state, m) {
    c.uint.preencode(state, m.corePointer)
    c.uint.preencode(state, m.dataPointer)
  },
  encode (state, m) {
    c.uint.encode(state, m.corePointer)
    c.uint.encode(state, m.dataPointer)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)

    return {
      corePointer: r0,
      dataPointer: r1
    }
  }
}

const encoding3_enum = {
  blake2b: 'blake2b'
}

// @core/hashes enum
const encoding3 = {
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

const encoding4_enum = {
  ed25519: 'ed25519'
}

// @core/signatures enum
const encoding4 = {
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
const encoding5 = {
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
const encoding6 = {
  preencode (state, m) {
    encoding4.preencode(state, m.signature)
    c.fixed32.preencode(state, m.namespace)
    c.fixed32.preencode(state, m.publicKey)
  },
  encode (state, m) {
    encoding4.encode(state, m.signature)
    c.fixed32.encode(state, m.namespace)
    c.fixed32.encode(state, m.publicKey)
  },
  decode (state) {
    const r0 = encoding4.decode(state)
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
const encoding7 = {
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
const encoding8_4 = c.array(encoding6)

// @core/manifest
const encoding8 = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    state.end++ // max flag is 2 so always one byte
    encoding3.preencode(state, m.hash)
    c.uint.preencode(state, m.quorum)
    encoding8_4.preencode(state, m.signers)

    if (m.prologue) encoding7.preencode(state, m.prologue)
  },
  encode (state, m) {
    const flags =
      (m.allowPatch ? 1 : 0) |
      (m.prologue ? 2 : 0)

    c.uint.encode(state, m.version)
    c.uint.encode(state, flags)
    encoding3.encode(state, m.hash)
    c.uint.encode(state, m.quorum)
    encoding8_4.encode(state, m.signers)

    if (m.prologue) encoding7.encode(state, m.prologue)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      hash: encoding3.decode(state),
      quorum: c.uint.decode(state),
      allowPatch: (flags & 1) !== 0,
      signers: encoding8_4.decode(state),
      prologue: (flags & 2) !== 0 ? encoding7.decode(state) : null
    }
  }
}

// @core/keyPair
const encoding9 = {
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
const encoding10_2 = c.frame(encoding8)

// @core/auth
const encoding10 = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.fixed32.preencode(state, m.discoveryKey)
    state.end++ // max flag is 4 so always one byte

    if (m.manifest) encoding10_2.preencode(state, m.manifest)
    if (m.keyPair) encoding9.preencode(state, m.keyPair)
    if (m.encryptionKey) c.buffer.preencode(state, m.encryptionKey)
  },
  encode (state, m) {
    const flags =
      (m.manifest ? 1 : 0) |
      (m.keyPair ? 2 : 0) |
      (m.encryptionKey ? 4 : 0)

    c.fixed32.encode(state, m.key)
    c.fixed32.encode(state, m.discoveryKey)
    c.uint.encode(state, flags)

    if (m.manifest) encoding10_2.encode(state, m.manifest)
    if (m.keyPair) encoding9.encode(state, m.keyPair)
    if (m.encryptionKey) c.buffer.encode(state, m.encryptionKey)
  },
  decode (state) {
    const r0 = c.fixed32.decode(state)
    const r1 = c.fixed32.decode(state)
    const flags = c.uint.decode(state)

    return {
      key: r0,
      discoveryKey: r1,
      manifest: (flags & 1) !== 0 ? encoding10_2.decode(state) : null,
      keyPair: (flags & 2) !== 0 ? encoding9.decode(state) : null,
      encryptionKey: (flags & 4) !== 0 ? c.buffer.decode(state) : null
    }
  }
}

// @core/head
const encoding11 = {
  preencode (state, m) {
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, m.length)
  },
  decode (state) {
    const r0 = c.uint.decode(state)

    return {
      length: r0
    }
  }
}

// @core/batches
const encoding12 = c.array({
  preencode (state, m) {
    c.string.preencode(state, m.name)
    c.uint.preencode(state, m.dataPointer)
  },
  encode (state, m) {
    c.string.encode(state, m.name)
    c.uint.encode(state, m.dataPointer)
  },
  decode (state) {
    const r0 = c.string.decode(state)
    const r1 = c.uint.decode(state)

    return {
      name: r0,
      dataPointer: r1
    }
  }
})

// @core/dependencies
const encoding13 = c.array({
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
    case '@core/hashes': return encoding3_enum
    case '@core/signatures': return encoding4_enum
    default: throw new Error('Enum not found ' + name)
  }
}

function getEncoding (name) {
  switch (name) {
    case '@corestore/allocated': return encoding0
    case '@corestore/head': return encoding1
    case '@corestore/core': return encoding2
    case '@core/hashes': return encoding3
    case '@core/signatures': return encoding4
    case '@core/tree-node': return encoding5
    case '@core/signer': return encoding6
    case '@core/prologue': return encoding7
    case '@core/manifest': return encoding8
    case '@core/keyPair': return encoding9
    case '@core/auth': return encoding10
    case '@core/head': return encoding11
    case '@core/batches': return encoding12
    case '@core/dependencies': return encoding13
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
