const Xache = require('xache')

// TODO: move diagnostics elswhere
globalThis.proofCache = { hit: 0, hitAsync: 0, miss: 0 }

class AsyncCache extends Xache {
  async get (key, fetch) {
    // return fetch() // disable cache

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

    const pending = fetch()
      .then((settled) => {
        if (settled !== null) {
          // update cache entry
          this.set(key, { pending: null, settled })
        } else {
          // discard null entries
          this.delete(key)
        }

        return settled
      })

    this.set(key, { pending, settled: null })

    return pending
  }
}

module.exports = AsyncCache
