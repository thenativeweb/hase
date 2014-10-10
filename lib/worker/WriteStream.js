'use strict';

var stream = require('stream'),
    util = require('util');

var Writable = stream.Writable;

var WorkerWritable = function (channel, name) {
  Writable.call(this, { objectMode: true });

  this.channel = channel;
  this.name = name;
};

util.inherits(WorkerWritable, Writable);

/*eslint-disable no-underscore-dangle*/
WorkerWritable.prototype._write = function (chunk, encoding, callback) {
/*eslint-enable no-underscore-dangle*/
  this.channel.publish(this.name, '', new Buffer(JSON.stringify(chunk), 'utf8'), {
    persistent: true
  });
  callback(null);
};

module.exports = WorkerWritable;
