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

  that.channel.assertExchange(that.name, 'fanout', { durable: true }, function (err) {
    if (err) {
      return callback(err);
    }

    that.channel.assertQueue('', { autoDelete: true, exclusive: true }, function (err, ok) {
      var generatedQueueName = ok.queue;

      if (err) {
        return callback(err);
      }

      that.channel.bindQueue(generatedQueueName, that.name, '', {}, function (err) {
        var readStream;

        if (err) {
          return callback(err);
        }

        that.channel.consume(generatedQueueName, function (message) {
          readStream.write(JSON.parse(message.content.toString('utf8')));
        }, { noAck: true }, function (err) {
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
