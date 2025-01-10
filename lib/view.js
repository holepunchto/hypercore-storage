const { Readable, getStreamError } = require('streamx')
const b4a = require('b4a')

class OverlayStream extends Readable {
  constructor (stream, start, end, reverse, changes, ranges) {
    super()

    this.start = start
    this.end = end
    this.reverse = reverse
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
    const key = entry.key

    while (this.range < this.ranges.length) {
      const r = this.ranges[this.range]

      // we moved past the range
      if (this.reverse ? b4a.compare(key, r[0]) < 0 : b4a.compare(r[2], key) <= 0) {
        this.range++
        continue
      }

      // we didnt move past and are in, drop
      if (b4a.compare(r[0], key) <= 0 && b4a.compare(key, r[2]) < 0) {
        return true
      }

      break
    }

    while (this.change < this.changes.length) {
      const c = this.changes[this.change]
      const key = c[0]
      const value = typeof c[1] === 'string' ? b4a.from(c[1]) : c[1]
      const cmp = b4a.compare(key, entry.key)

      // same value, if not deleted, return new one
      if (cmp === 0) {
        this.change++
        return value === null || this._inRange(key) === false ? true : this.push({ key, value })
      }

      // we moved past the change, push it
      if (this.reverse ? cmp > 0 : cmp < 0) {
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

class Overlay {
  constructor () {
    this.indexed = 0
    this.changes = null
    this.ranges = null
    this.reverse = false
  }

  update (view, reverse) {
    if (view.indexed === this.indexed) return

    const changes = view.map === null ? [] : [...view.map.values()]
    const ranges = view.ranges === null ? [] : view.ranges.slice(0)

    const cmp = reverse ? cmpChangeReverse : cmpChange

    changes.sort(cmp)
    ranges.sort(cmp)

    this.indexed = view.indexed
    this.changes = changes
    this.ranges = ranges
    this.reverse = reverse
  }

  createStream (stream, start, end, reverse) {
    return new OverlayStream(
      stream,
      start,
      end,
      reverse,
      this.reverse === reverse ? this.changes : reverseArray(this.changes),
      this.reverse === reverse ? this.ranges : reverseArray(this.ranges)
    )
  }
}

class View {
  constructor () {
    this.map = null
    this.indexed = 0
    this.changes = null
    this.ranges = null
    this.overlay = null
    this.snap = null
    this.readers = 0
  }

  snapshot () {
    if (this._attached()) return this.snap.snapshot()

    const snap = new View()

    snap.map = this.map
    snap.indexed = this.indexed
    snap.changes = this.changes
    snap.ranges = this.ranges

    if (this._frozen()) return snap

    this.readers++
    snap.snap = this

    return snap
  }

  readStart () {
    if (this.snap !== null) this.readers++
  }

  readStop () {
    if (this.snap !== null && --this.readers === 0) this.snap.readers--
  }

  size () {
    return this.changes === null ? 0 : this.changes.length
  }

  updated () {
    return this.changes === null
  }

  get (read, key) {
    return this.changes === null ? read.get(key) : this._indexAndGet(read, key)
  }

  reset () {
    this.indexed = 0
    this.snap = this.map = this.changes = this.ranges = null
  }

  iterator (db, start, end, reverse) {
    const stream = db.iterator({ gte: start, lt: end, reverse })
    if (this.changes === null) return stream

    this._index()

    if (this.overlay === null) this.overlay = new Overlay()
    this.overlay.update(this, reverse)
    return this.overlay.createStream(stream, start, end, reverse)
  }

  _indexAndGet (read, key) {
    this._index()
    const change = this.map.get(b4a.toString(key, 'hex'))
    if (change === undefined) return read.get(key)
    if (typeof change[1] === 'string') return Promise.resolve(b4a.from(change[1]))
    return Promise.resolve(change[1])
  }

  _attached () {
    return this.snap !== null && this.changes === this.snap.changes
  }

  _frozen () {
    return this.changes === null || (this.snap !== null && this.changes !== this.snap.changes)
  }

  _index () {
    // if we are a snap and we are still attached (ie no mutations), simply copy the refs
    if (this._attached()) {
      this.snap._index()
      this.map = this.snap.map
      this.ranges = this.snap.ranges
      this.indexed = this.snap.indexed
      return
    }

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

  apply (changes) {
    if (this.snap !== null) throw new Error('Illegal to push changes to a snapshot')

    if (this.readers !== 0 && this.changes !== null) {
      this.changes = this.changes.slice(0)
      this.ranges = this.ranges === null ? null : this.ranges.slice(0)
      this.map = this.map === null ? null : new Map([...this.map])
    }

    if (this.changes === null) {
      this.changes = changes
      return
    }

    for (let i = 0; i < changes.length; i++) {
      this.changes.push(changes[i])
    }
  }

  static async flush (changes, db) {
    if (changes === null) return true

    const w = db.write({ autoDestroy: true })

    for (const [start, value, end] of changes) {
      if (end !== null) w.tryDeleteRange(start, end)
      else if (value !== null) w.tryPut(start, value)
      else w.tryDelete(start)
    }

    await w.flush()

    return true
  }
}

module.exports = View

function cmpChange (a, b) {
  const c = b4a.compare(a[0], b[0])
  return c === 0 ? b4a.compare(a[2], b[2]) : c
}

function cmpChangeReverse (a, b) {
  return cmpChange(b, a)
}

function noop () {}

function reverseArray (list) {
  const r = new Array(list.length)
  for (let i = 0; i < list.length; i++) r[r.length - 1 - i] = list[i]
  return r
}
