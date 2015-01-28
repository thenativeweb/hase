'use strict';

var stream = require('stream');

var WriteStream = require('./WriteStream');

var PassThrough = stream.PassThrough;

var Publisher = function (channel, name) {
  this.channel = channel;
  this.name = name;
};

Publisher.prototype.createWriteStream = function (callback) {
  var that = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  that.channel.assertExchange(that.name, 'fanout', { durable: true }, function (err) {
    if (err) {
      return callback(err);
    }

    callback(null, new WriteStream(that.channel, that.name));
  });
};

Publisher.prototype.createReadStream = function (callback) {
  var that = this;

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  that.channel.assertExchange(that.name, 'fanout', { durable: true }, function (errAssertExchange) {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    that.channel.assertQueue('', { autoDelete: true, exclusive: true }, function (errAssertQueue, ok) {
      var generatedQueueName = ok.queue;

      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      that.channel.bindQueue(generatedQueueName, that.name, '', {}, function (errBindQueue) {
        var readStream;

        if (errBindQueue) {
          return callback(errBindQueue);
        }

        that.channel.consume(generatedQueueName, function (message) {
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

module.exports = Publisher;
