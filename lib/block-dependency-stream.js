const { Readable, getStreamError } = require('streamx')
const { core } = require('./keys')

module.exports = class BlockStream extends Readable {
  constructor (core, db, updates, start, end, reverse) {
    super()

    this.core = core
    this.db = db
    this.updates = updates
    this.start = start
    this.end = end
    this.reverse = reverse === true

    this._drained = true
    this._consumed = 0
    this._stream = null
    this._oncloseBound = this._onclose.bind(this)
    this._maybeDrainBound = this._maybeDrain.bind(this)

    this._update()
  }

  _update () {
    if (this._consumed > this.core.dependencies.length) return

    const deps = this.core.dependencies
    const index = this._findDependencyIndex(deps)

    const curr = index < deps.length ? deps[index] : null
    const prev = (index > 0 && index - 1 < deps.length) ? deps[index - 1] : null

    const start = (prev && prev.length > this.start) ? prev.length : this.start
    const end = (curr && (this.end === -1 || curr.length < this.end)) ? curr.length : this.end

    const ptr = curr ? curr.dataPointer : this.core.dataPointer

    this._makeStream(core.block(ptr, start), core.block(ptr, end))
  }

  _findDependencyIndex (deps) {
    if (!this.reverse) return this._consumed++

    let i = deps.length - this._consumed++
    while (i > 0) {
      if (deps[i - 1].length <= this.end) return i
      i--
      this._consumed++
    }

    return 0
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

    this._stream = null

    this._update()
    this._maybeDrain()
  }

  _makeStream (start, end) {
    this._stream = this.updates.iterator(this.db, start, end, this.reverse)
    this._stream.on('readable', this._maybeDrainBound)
    this._stream.on('error', noop)
    this._stream.on('close', this._oncloseBound)
  }
}

function noop () {}
