const Xache = require('xache')

// TODO: move diagnostics elswhere
globalThis.proofCache = { hit: 0, hitAsync: 0, miss: 0 }

class AsyncCache extends Xache {
  counter = 0
  refs = 0
  destroyed = false

  ref() {
    this.refs++
  }

  set(key, value) {
    if (this.destroyed) throw new Error('TMP UNREACHABLE')
    super.set(key, value)
  }

  async get(key, fetch) {
    // return fetch() // disables cache
    if (this.destroyed) throw new Error('TMP UNREACHABLE')

    const value = super.get(key)

    if (value) {
      if (value.settled) {
        globalThis.proofCache.hit++
      } else {
        globalThis.proofCache.hitAsync++
      }

      return value.settled || value.pending
    }

    globalThis.proofCache.miss++

    const version = this.counter
    const entry = { pending: null, settled: null }

    entry.pending = fetch()
      .then((settled) => {
        const cached = super.get(key)

        // preflight is gone; no-op
        if (cached !== entry) return settled

        if (settled !== null && version === this.counter) {
          // replace cached entry with value
          this.set(key, { pending: null, settled })
        } else {
          // discard null & invalidated entries
          this.delete(key)
        }

        return settled
      })
      .catch((err) => {
        const cached = super.get(key)
        if (cached === entry) {
          this.delete(key)
        }

        throw err
      })

    this.set(key, entry)

    return entry.pending
  }

  invalidate() {
    this.counter++
    this.clear()
  }

  destroy() {
    if (--this.refs > 0) return

    super.destroy()
    this.destroyed = true
  }
}

module.exports = AsyncCache
