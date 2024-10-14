const { Readable, isEnded, getStreamError } = require('streamx')

module.exports = class DependencyStream extends Readable {
  constructor (storage, createStream, opts = {}) {
    super()

    this.storage = storage
    this.createStream = createStream

    let max = 0

    const reverse = !!opts.reverse
    const limit = opts.limit === 0 ? 0 : (opts.limit || Infinity)
    const gte = typeof opts.gte === 'number' ? opts.gte : typeof opts.gt === 'number' ? opts.gt + 1 : 0
    const lt = typeof opts.lt === 'number' ? opts.lt : typeof opts.lte === 'number' ? opts.lte + 1 : Infinity

    const streams = []

    for (let i = 0; i < storage.dependencies.length; i++) {
      const min = max
      max += storage.dependencies[i].length

      streams.push({
        data: storage.dependencies[i].data,
        gte: Math.max(gte, min),
        lt: Math.min(lt, max)
      })
    }

    streams.push({
      data: storage.dataPointer,
      gte: Math.max(gte, max),
      lt
    })

    this._streams = reverse ? streams.reverse() : streams
    this._reverse = reverse
    this._limit = limit
    this._next = 0
    this._active = null
    this._pendingDestroy = null
    this._ondataBound = this._ondata.bind(this)
    this._oncloseBound = this._onclose.bind(this)

    this._nextStream()
  }

  _read (cb) {
    this._active.resume()
    cb(null)
  }

  _predestroy () {
    if (this._active) this._active.destroy()
  }

  _destroy (cb) {
    if (this._active === null) cb(null)
    else this._pendingDestroy = cb
  }

  _ondata (data) {
    if (this._limit > 0) this._limit--
    if (this.push(data) === false) this._active.pause()
  }

  _onclose () {
    if (!isEnded(this._active)) {
      const error = getStreamError(this._active)
      this._active = null
      this.destroy(error)
      this._continueDestroy(error)
      return
    }

    if (this._next >= this._streams.length || this._limit === 0) {
      this._active = null
      this.push(null)
      this._continueDestroy(null)
      return
    }

    this._nextStream()
  }

  _continueDestroy (err) {
    if (this._pendingDestroy === null) return
    const cb = this._pendingDestroy
    this._pendingDestroy = null
    cb(err)
  }

  _nextStream () {
    const { data, gte, lt } = this._streams[this._next++]

    const stream = this.createStream(this.storage.db, this.storage.dbSnapshot, data, {
      reverse: this._reverse,
      limit: this._limit,
      gte,
      lt
    })

    this._active = stream

    stream.on('data', this._ondataBound)
    stream.on('error', noop) // handled in onclose
    stream.on('close', this._oncloseBound)
  }
}

function noop () {}
