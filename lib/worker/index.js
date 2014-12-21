'use strict';

var stream = require('stream');

var WriteStream = require('./WriteStream');

var PassThrough = stream.PassThrough;

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

Worker.prototype.createReadStream = function (callback) {
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
        var readStream;

        if (err) {
          return callback(err);
        }

        that.channel.consume(that.name, function (message) {
          var parsedMessage = {};

          parsedMessage.payload = JSON.parse(message.content.toString('utf8'));

          parsedMessage.next = function () {
            that.channel.ack(message);
          };

          parsedMessage.discard = function () {
            that.channel.nack(message, false, false);
          };

          parsedMessage.defer = function () {
            that.channel.nack(message, false, true);
          };

          readStream.write(parsedMessage);
        }, {}, function (err) {
          if (err) {
            return callback(err);
          }

          readStream = new PassThrough({ objectMode: true });
          callback(null, readStream);
        });
      });
    });
  });
};

module.exports = Worker;
