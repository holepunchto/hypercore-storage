const MIN_GAP = 25
const MAX_PENDING = 4096

module.exports = class FsyncTracker {
  constructor(db) {
    this.db = db
    this.completed = 0
    this.started = 0
    this.running = null
    this.lastRunAt = 0
    this.pending = new Map()
  }

  needed(ptr) {
    const needed = this.pending.get(ptr)
    if (needed !== undefined) return needed

    return 1 // 1 guarantees fsync called at least once on store open
  }

  mark(ptr) {
    const needed = this.started + 1
    this.pending.set(ptr, needed)

    if (this.pending.size >= MAX_PENDING) this.flush(needed).catch(noop)

    return needed
  }

  async flush(target = this.started + 1) {
    while (this.completed < target) {
      if (this.db.closed) throw new Error('Storage has closed')
      await this._sync(target)
    }
  }

  async _sync(target) {
    if (this.running !== null) {
      if (target <= this.running.gen) return this.running.promise
      await this.running.promise.catch(noop)
      return
    }

    const gen = this.started + 1
    const promise = this._run(gen)
    this.running = { gen, promise }

    try {
      await promise
    } finally {
      this.running = null
    }
  }

  async _run(gen) {
    const gap = MIN_GAP - (Date.now() - this.lastRunAt)
    if (gap > 0) await sleep(gap)

    this.started = gen
    await this.db.flushWAL()

    this.lastRunAt = Date.now()
    this.completed = gen

    for (const [ptr, needed] of this.pending) {
      if (needed <= gen) this.pending.delete(ptr)
    }
  }
}

function noop() {}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
