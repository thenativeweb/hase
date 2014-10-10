'use strict';

var WriteStream = require('./WriteStream');

var Worker = function (channel, name) {
  this.channel = channel;
  this.name = name;
};

Worker.prototype.createWriteStream = function (callback) {
  var that = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  that.channel.assertExchange(that.name, 'direct', { durable: true }, function (err) {
    if (err) {
      return callback(err);
    }

    that.channel.assertQueue(that.name, { durable: true }, function (err) {
      if (err) {
        return callback(err);
      }

      that.channel.bindQueue(that.name, that.name, '', {}, function (err) {
        if (err) {
          return callback(err);
        }

        callback(null, new WriteStream(that.channel, that.name));
      });
    });
  });
};

module.exports = Worker;
