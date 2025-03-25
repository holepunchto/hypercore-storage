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
    if (m.defaultDiscoveryKey) c.fixed32.preencode(state, m.defaultDiscoveryKey)
  },
  encode (state, m) {
    const flags =
      (m.allocated ? 1 : 0) |
      (m.seed ? 2 : 0) |
      (m.defaultDiscoveryKey ? 4 : 0)

    c.uint.encode(state, m.version)
    c.uint.encode(state, flags)

    if (m.allocated) encoding0.encode(state, m.allocated)
    if (m.seed) c.fixed32.encode(state, m.seed)
    if (m.defaultDiscoveryKey) c.fixed32.encode(state, m.defaultDiscoveryKey)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      allocated: (flags & 1) !== 0 ? encoding0.decode(state) : null,
      seed: (flags & 2) !== 0 ? c.fixed32.decode(state) : null,
      defaultDiscoveryKey: (flags & 4) !== 0 ? c.fixed32.decode(state) : null
    }
  }
}

// @corestore/alias
const encoding2 = {
  preencode (state, m) {
    c.string.preencode(state, m.name)
    c.fixed32.preencode(state, m.namespace)
  },
  encode (state, m) {
    c.string.encode(state, m.name)
    c.fixed32.encode(state, m.namespace)
  },
  decode (state) {
    const r0 = c.string.decode(state)
    const r1 = c.fixed32.decode(state)

    return {
      name: r0,
      namespace: r1
    }
  }
}

// @corestore/core
const encoding3 = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    c.uint.preencode(state, m.corePointer)
    c.uint.preencode(state, m.dataPointer)
    state.end++ // max flag is 1 so always one byte

    if (m.alias) encoding2.preencode(state, m.alias)
  },
  encode (state, m) {
    const flags = m.alias ? 1 : 0

    c.uint.encode(state, m.version)
    c.uint.encode(state, m.corePointer)
    c.uint.encode(state, m.dataPointer)
    c.uint.encode(state, flags)

    if (m.alias) encoding2.encode(state, m.alias)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)
    const r2 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      corePointer: r1,
      dataPointer: r2,
      alias: (flags & 1) !== 0 ? encoding2.decode(state) : null
    }
  }
}

const encoding4_enum = {
  blake2b: 'blake2b'
}

// @core/hashes enum
const encoding4 = {
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

const encoding5_enum = {
  ed25519: 'ed25519'
}

// @core/signatures enum
const encoding5 = {
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
const encoding6 = {
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
const encoding7 = {
  preencode (state, m) {
    encoding5.preencode(state, m.signature)
    c.fixed32.preencode(state, m.namespace)
    c.fixed32.preencode(state, m.publicKey)
  },
  encode (state, m) {
    encoding5.encode(state, m.signature)
    c.fixed32.encode(state, m.namespace)
    c.fixed32.encode(state, m.publicKey)
  },
  decode (state) {
    const r0 = encoding5.decode(state)
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
const encoding8 = {
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
const encoding9_4 = c.array(encoding7)
// @core/manifest.linked
const encoding9_6 = c.array(c.fixed32)

// @core/manifest
const encoding9 = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    state.end++ // max flag is 4 so always one byte
    encoding4.preencode(state, m.hash)
    c.uint.preencode(state, m.quorum)
    encoding9_4.preencode(state, m.signers)

    if (m.prologue) encoding8.preencode(state, m.prologue)
    if (m.linked) encoding9_6.preencode(state, m.linked)
  },
  encode (state, m) {
    const flags =
      (m.allowPatch ? 1 : 0) |
      (m.prologue ? 2 : 0) |
      (m.linked ? 4 : 0)

    c.uint.encode(state, m.version)
    c.uint.encode(state, flags)
    encoding4.encode(state, m.hash)
    c.uint.encode(state, m.quorum)
    encoding9_4.encode(state, m.signers)

    if (m.prologue) encoding8.encode(state, m.prologue)
    if (m.linked) encoding9_6.encode(state, m.linked)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: r0,
      hash: encoding4.decode(state),
      quorum: c.uint.decode(state),
      allowPatch: (flags & 1) !== 0,
      signers: encoding9_4.decode(state),
      prologue: (flags & 2) !== 0 ? encoding8.decode(state) : null,
      linked: (flags & 4) !== 0 ? encoding9_6.decode(state) : null
    }
  }
}

// @core/keyPair
const encoding10 = {
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
const encoding11_2 = c.frame(encoding9)

// @core/auth
const encoding11 = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.fixed32.preencode(state, m.discoveryKey)
    state.end++ // max flag is 4 so always one byte

    if (m.manifest) encoding11_2.preencode(state, m.manifest)
    if (m.keyPair) encoding10.preencode(state, m.keyPair)
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

    if (m.manifest) encoding11_2.encode(state, m.manifest)
    if (m.keyPair) encoding10.encode(state, m.keyPair)
    if (m.encryptionKey) c.buffer.encode(state, m.encryptionKey)
  },
  decode (state) {
    const r0 = c.fixed32.decode(state)
    const r1 = c.fixed32.decode(state)
    const flags = c.uint.decode(state)

    return {
      key: r0,
      discoveryKey: r1,
      manifest: (flags & 1) !== 0 ? encoding11_2.decode(state) : null,
      keyPair: (flags & 2) !== 0 ? encoding10.decode(state) : null,
      encryptionKey: (flags & 4) !== 0 ? c.buffer.decode(state) : null
    }
  }
}

// @core/head
const encoding12 = {
  preencode (state, m) {
    c.uint.preencode(state, m.fork)
    c.uint.preencode(state, m.length)
    c.fixed32.preencode(state, m.rootHash)
    c.buffer.preencode(state, m.signature)
  },
  encode (state, m) {
    c.uint.encode(state, m.fork)
    c.uint.encode(state, m.length)
    c.fixed32.encode(state, m.rootHash)
    c.buffer.encode(state, m.signature)
  },
  decode (state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)
    const r2 = c.fixed32.decode(state)
    const r3 = c.buffer.decode(state)

    return {
      fork: r0,
      length: r1,
      rootHash: r2,
      signature: r3
    }
  }
}

// @core/hints
const encoding13 = {
  preencode (state, m) {
    state.end++ // max flag is 1 so always one byte

    if (m.contiguousLength) c.uint.preencode(state, m.contiguousLength)
  },
  encode (state, m) {
    const flags = m.contiguousLength ? 1 : 0

    c.uint.encode(state, flags)

    if (m.contiguousLength) c.uint.encode(state, m.contiguousLength)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      contiguousLength: (flags & 1) !== 0 ? c.uint.decode(state) : 0
    }
  }
}

// @core/session
const encoding14 = {
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
}

// @core/sessions
const encoding15 = c.array(encoding14)

// @core/dependency
const encoding16 = {
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
}

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
    case '@core/hashes': return encoding4_enum
    case '@core/signatures': return encoding5_enum
    default: throw new Error('Enum not found ' + name)
  }
}

function getEncoding (name) {
  switch (name) {
    case '@corestore/allocated': return encoding0
    case '@corestore/head': return encoding1
    case '@corestore/alias': return encoding2
    case '@corestore/core': return encoding3
    case '@core/hashes': return encoding4
    case '@core/signatures': return encoding5
    case '@core/tree-node': return encoding6
    case '@core/signer': return encoding7
    case '@core/prologue': return encoding8
    case '@core/manifest': return encoding9
    case '@core/keyPair': return encoding10
    case '@core/auth': return encoding11
    case '@core/head': return encoding12
    case '@core/hints': return encoding13
    case '@core/session': return encoding14
    case '@core/sessions': return encoding15
    case '@core/dependency': return encoding16
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

const resolveStruct = getStruct // compat

module.exports = { resolveStruct, getStruct, getEnum, getEncoding, encode, decode, setVersion, version }
