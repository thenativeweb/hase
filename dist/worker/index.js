'use strict';

var stream = require('stream');

var WriteStream = require('./WriteStream');

var PassThrough = stream.PassThrough;

var Worker = function Worker(channel, name) {
  this.channel = channel;
  this.name = name;
};

Worker.prototype.createWriteStream = function (callback) {
  var _this = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'direct', { durable: true }, function (errAssertExchange) {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    _this.channel.assertQueue(_this.name, { durable: true }, function (errAssertQueue) {
      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      _this.channel.bindQueue(_this.name, _this.name, '', {}, function (err) {
        if (err) {
          return callback(err);
        }

        callback(null, new WriteStream(_this.channel, _this.name));
      });
    });
  });
};

Worker.prototype.createReadStream = function (callback) {
  var _this2 = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'direct', { durable: true }, function (errAssertExchange) {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    _this2.channel.assertQueue(_this2.name, { durable: true }, function (errAssertQueue) {
      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      _this2.channel.bindQueue(_this2.name, _this2.name, '', {}, function (errBindQueue) {
        if (errBindQueue) {
          return callback(errBindQueue);
        }

        var readStream = new PassThrough({ objectMode: true });

        _this2.channel.consume(_this2.name, function (message) {
          var parsedMessage = {};

          parsedMessage.payload = JSON.parse(message.content.toString('utf8'));

          parsedMessage.next = function () {
            _this2.channel.ack(message);
          };

          parsedMessage.discard = function () {
            _this2.channel.nack(message, false, false);
          };

          parsedMessage.defer = function () {
            _this2.channel.nack(message, false, true);
          };

          readStream.write(parsedMessage);
        }, {}, function (err) {
          if (err) {
            return callback(err);
          }
          callback(null, readStream);
        });
      });
    });
  });
};

module.exports = Worker;