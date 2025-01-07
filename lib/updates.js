const { Readable, getStreamError } = require('streamx')
const b4a = require('b4a')

class OverlayStream extends Readable {
  constructor (stream, start, end, changes, ranges) {
    super()

    this.start = start
    this.end = end
    this.changes = changes
    this.ranges = ranges
    this.change = 0
    this.range = 0

    this._stream = stream
    this._drained = false

    this._stream.on('readable', this._drainMaybe.bind(this))
    this._stream.on('error', noop)
    this._stream.on('close', this._onclose.bind(this))
  }

  _drainMaybe () {
    if (this._drained === true) return
    this._drained = this._onreadable()
  }

  _onclose () {
    if (this.destroying) return

    const err = getStreamError(this._stream)

    if (err !== null) {
      this.destroy(err)
      return
    }

    while (this.change < this.changes.length) {
      const c = this.changes[this.change++]
      const key = c[0]
      const value = c[1]

      if (value !== null && this._inRange(key)) this.push({ key, value })
    }

    this.push(null)
    this._stream = null
  }

  _onreadable () {
    let data = this._stream.read()

    if (data === null) return false

    do {
      this._push(data)
      data = this._stream.read()
    } while (data !== null)

    return true
  }

  _read (cb) {
    this._drained = this._onreadable()
    cb(null)
  }

  _predestroy () {
    this.stream.destroy()
  }

  _push (entry) {
    while (this.range < this.ranges.length) {
      const r = this.ranges[this.range]

      // we moved past the range
      if (b4a.compare(r[2], entry.key) <= 0) {
        this.range++
        continue
      }

      // we didnt move past and are in, drop
      if (b4a.compare(r[0], entry.key) <= 0) {
        return true
      }

      break
    }

    while (this.change < this.changes.length) {
      const c = this.changes[this.change]
      const key = c[0]
      const value = c[1]
      const cmp = b4a.compare(key, entry.key)

      // same value, if not deleted, return new one
      if (cmp === 0) {
        this.change++
        return value === null || this._inRange(key) === false ? true : this.push({ key, value })
      }

      // we moved past the change, push it
      if (cmp < 0) {
        this.change++
        if (value !== null && this._inRange(key) === true) this.push({ key, value })
        continue
      }

      return this.push(entry)
    }

    return this.push(entry)
  }

  _inRange (key) {
    return b4a.compare(this.start, key) <= 0 && b4a.compare(key, this.end) < 0
  }
}

module.exports = class Updates {
  constructor () {
    this.map = null
    this.indexed = 0
    this.changes = null
    this.ranges = null
  }

  size () {
    return this.changes === null ? 0 : this.changes.length
  }

  updated () {
    return this.changes === null
  }

  put (key, value) {
    if (this.changes === null) this.changes = []
    this.changes.push([key, value, null])
  }

  deleteRange (start, end) {
    if (this.changes === null) this.changes = []
    this.changes.push([start, null, end])
  }

  delete (key) {
    if (this.changes === null) this.changes = []
    this.changes.push([key, null, null])
  }

  get (read, key) {
    return this.changes === null ? read.get(key) : this._indexAndGet(read, key)
  }

  reset () {
    this.indexed = 0
    this.map = this.changes = this.ranges = null
  }

  iterator (db, start, end) {
    const stream = db.iterator({ gte: start, lt: end })
    if (this.changes === null) return stream

    const changes = [...this.map.values()]
    const ranges = this.ranges === null ? [] : this.ranges.slice(0)

    changes.sort(cmpChange)
    ranges.sort(cmpChange)

    return new OverlayStream(stream, start, end, changes, ranges)
  }

  _indexAndGet (read, key) {
    this._index()
    const change = this.map.get(b4a.toString(key, 'hex'))
    return change === undefined ? read.get(key) : Promise.resolve(change[1])
  }

  _index () {
    if (this.changes.length === this.indexed) return
    if (this.map === null) this.map = new Map()

    while (this.indexed < this.changes.length) {
      const c = this.changes[this.indexed++]

      if (c[2] === null) this.map.set(b4a.toString(c[0], 'hex'), c)
      else this._indexRange(c)
    }
  }

  _indexRange (range) {
    const s = b4a.toString(range[0], 'hex')
    const e = b4a.toString(range[2], 'hex')

    for (const key of this.map.keys()) {
      if (s <= key && key < e) this.map.delete(key)
    }

    if (this.ranges === null) this.ranges = []
    this.ranges.push(range)
  }

  flush (db) {
    if (this.changes === null) return Promise.resolve()
    const w = db.write({ autoDestroy: true })
    for (const [start, value, end] of this.changes) {
      if (end !== null) w.tryDeleteRange(start, end)
      else if (value !== null) w.tryPut(start, value)
      else w.tryDelete(start)
    }
    return w.flush()
  }
}

function cmpChange (a, b) {
  const c = b4a.compare(a[0], b[0])
  return c === 0 ? b4a.compare(a[2], b[2]) : c
}

function noop () {}
