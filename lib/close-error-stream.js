const { Readable } = require('streamx')

// used for returned a stream that just errors (during read during teardown)

module.exports = class CloseErrorStream extends Readable {
  constructor (err) {
    super()
    this.error = err
  }

  _open (cb) {
    cb(this.error)
  }
}
