const c = require('compact-encoding')

const head = {
  preencode(state, m) {
    c.uint.preencode(state, m.fork)
    c.uint.preencode(state, m.length)
    c.fixed32.preencode(state, m.rootHash)
    c.optionalBuffer.preencode(state, m.signature)
  },
  encode(state, m) {
    c.uint.encode(state, m.fork)
    c.uint.encode(state, m.length)
    c.fixed32.encode(state, m.rootHash)
    c.optionalBuffer.encode(state, m.signature)
  },
  decode(state) {
    const r0 = c.uint.decode(state)
    const r1 = c.uint.decode(state)
    const r2 = c.fixed32.decode(state)
    const r3 = c.optionalBuffer.decode(state)

    return {
      fork: r0,
      length: r1,
      rootHash: r2,
      signature: r3
    }
  }
}

module.exports = { head }
