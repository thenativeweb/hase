'use strict';

var stream = require('stream');

var WriteStream = require('./WriteStream');

var PassThrough = stream.PassThrough;

var Publisher = function Publisher(channel, name) {
  this.channel = channel;
  this.name = name;
};

Publisher.prototype.createWriteStream = function (callback) {
  var _this = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'fanout', { durable: true }, function (err) {
    if (err) {
      return callback(err);
    }
    callback(null, new WriteStream(_this.channel, _this.name));
  });
};

Publisher.prototype.createReadStream = function (callback) {
  var _this2 = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'fanout', { durable: true }, function (errAssertExchange) {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    _this2.channel.assertQueue('', { autoDelete: true, exclusive: true }, function (errAssertQueue, ok) {
      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      var generatedQueueName = ok.queue;

      _this2.channel.bindQueue(generatedQueueName, _this2.name, '', {}, function (errBindQueue) {
        if (errBindQueue) {
          return callback(errBindQueue);
        }

        var readStream = new PassThrough({ objectMode: true });

        _this2.channel.consume(generatedQueueName, function (message) {
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

module.exports = Publisher;