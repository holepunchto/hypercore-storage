const c = require('compact-encoding')

exports.DiscoveryKey = {
  preencode (state, m) {
    c.uint.preencode(state, m.header)
    c.uint.preencode(state, m.data)
  },
  encode (state, m) {
    c.uint.encode(state, m.header)
    c.uint.encode(state, m.data)
  },
  decode (state) {
    return {
      header: c.uint.decode(state),
      data: c.uint.decode(state)
    }
  }
}

exports.TreeNode = {
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
    return {
      index: c.uint.decode(state),
      size: c.uint.decode(state),
      hash: c.fixed32.decode(state)
    }
  }
}
