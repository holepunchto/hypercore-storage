const { Readable, getStreamError } = require('streamx')
const { core } = require('./keys')

module.exports = class BlockStream extends Readable {
  constructor (core, db, updates, start, end) {
    super({ mapReadable })

    this.core = core
    this.db = db
    this.updates = updates
    this.end = end

    this._drained = true
    this._consumed = 0
    this._stream = null
    this._oncloseBound = this._onclose.bind(this)
    this._maybeDrainBound = this._maybeDrain.bind(this)

    this._update()
  }

  _update () {
    if (this._consumed > this.core.dependencies.length) return

    const offset = this._consumed === 0 ? 0 : this.core.dependencies[this._consumed - 1].length

    let end = 0
    let ptr = 0

    if (this._consumed < this.core.dependencies.length) {
      const dep = this.core.dependencies[this._consumed]
      end = this.end === -1 ? dep.length : Math.min(this.end, dep.length)
      ptr = dep.dataPointer
    } else {
      end = this.end === -1 ? Infinity : this.end
      ptr = this.core.dataPointer
    }

    this._consumed++

    if (end === offset) return

    this._makeStream(core.block(ptr, offset), core.block(ptr, end))
  }

  _predestroy () {
    if (this._stream !== null) this._stream.destroy()
  }

  _read (cb) {
    this._drained = this._onreadable()
    cb(null)
  }

  _maybeDrain () {
    if (this._drained === true) return
    this._drained = this._onreadable()
  }

  _onreadable () {
    if (this._stream === null) {
      this.push(null)
      return true
    }

    let data = this._stream.read()

    if (data === null) return false

    do {
      this.push(data)
      data = this._stream.read()
    } while (data !== null)

    return true
  }

  _onclose () {
    if (this.destroying) return

    const err = getStreamError(this._stream)

    if (err !== null) {
      this.destroy(err)
      return
    }

    // empty the current stream
    if (this._onreadable() === true) this._drained = true

    this._stream = this._start = this._end = null

    this._update()
    this._maybeDrain()
  }

  _makeStream (start, end) {
    this._stream = this.updates.iterator(this.db, start, end)
    this._stream.on('readable', this._maybeDrainBound)
    this._stream.on('error', noop)
    this._stream.on('close', this._oncloseBound)
  }
}

function noop () {}

function mapReadable (entry) {
  return { index: core.blockIndex(entry.key), value: entry.value }
}
