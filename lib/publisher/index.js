'use strict';

const stream = require('stream');

const WriteStream = require('./WriteStream');

const PassThrough = stream.PassThrough;

const Publisher = function (channel, name) {
  this.channel = channel;
  this.name = name;
};

Publisher.prototype.createWriteStream = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'fanout', { durable: true }, err => {
    if (err) {
      return callback(err);
    }
    callback(null, new WriteStream(this.channel, this.name));
  });
};

Publisher.prototype.createReadStream = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'fanout', { durable: true }, errAssertExchange => {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    this.channel.assertQueue('', { autoDelete: true, exclusive: true }, (errAssertQueue, ok) => {
      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      const generatedQueueName = ok.queue;

      this.channel.bindQueue(generatedQueueName, this.name, '', {}, errBindQueue => {
        if (errBindQueue) {
          return callback(errBindQueue);
        }

        const readStream = new PassThrough({ objectMode: true });

        this.channel.consume(generatedQueueName, message => {
          const parsedMessage = {};

          parsedMessage.payload = JSON.parse(message.content.toString('utf8'));

          parsedMessage.next = () => {
            this.channel.ack(message);
          };

          parsedMessage.discard = () => {
            this.channel.nack(message, false, false);
          };

          parsedMessage.defer = () => {
            this.channel.nack(message, false, true);
          };

          readStream.write(parsedMessage);
        }, {}, err => {
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
