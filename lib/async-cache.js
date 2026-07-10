const Xache = require('xache')

class AsyncCache extends Xache {
  refs = 0

  ref() {
    this.refs++
  }

  unref() {
    if (--this.refs) return
    this.destroy()
  }
}

module.exports = AsyncCache
