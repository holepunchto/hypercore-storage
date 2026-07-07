const Xache = require('xache')

class AsyncCache extends Xache {
  refs = 0

  static stats = { hit: 0, hitAsync: 0, miss: 0 }
  get stats() {
    return AsyncCache.stats
  }

  ref() {
    this.refs++
  }

  unref() {
    if (--this.refs) return
    this.destroy()
  }

  async get(key, fetch) {
    // cache-hit
    if (this.has(key)) {
      const { pending, value } = super.get(key)

      if (pending) {
        AsyncCache.stats.hitAsync++
        // promise-deduped
        return pending
      } else {
        AsyncCache.stats.hit++
        return value
      }
    }

    // cache-miss
    AsyncCache.stats.miss++
    const entry = { pending: null, value: null }

    const promise = fetch()
    entry.pending = this._resolve(key, promise, entry)

    this.set(key, entry)

    return entry.pending
  }

  async _resolve(key, promise, entry) {
    try {
      const value = await promise
      const cached = super.get(key)

      // preflight is gone; no-op
      if (cached !== entry) return value

      if (value !== null) {
        // replace cached entry with value
        this.set(key, { pending: null, value })
      } else {
        // discard non-existing values
        this.delete(key)
      }

      return value
    } catch (err) {
      // cleanup
      const cached = super.get(key)
      if (cached === entry) {
        this.delete(key)
      }

      throw err
    }
  }
}

module.exports = AsyncCache
