'use strict';

const stream = require('stream');

const WriteStream = require('./WriteStream');

const PassThrough = stream.PassThrough;

const Worker = function (channel, name) {
  this.channel = channel;
  this.name = name;
};

Worker.prototype.createWriteStream = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'direct', { durable: true }, errAssertExchange => {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    this.channel.assertQueue(this.name, { durable: true }, errAssertQueue => {
      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      this.channel.bindQueue(this.name, this.name, '', {}, err => {
        if (err) {
          return callback(err);
        }

        callback(null, new WriteStream(this.channel, this.name));
      });
    });
  });
};

Worker.prototype.createReadStream = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.channel.assertExchange(this.name, 'direct', { durable: true }, errAssertExchange => {
    if (errAssertExchange) {
      return callback(errAssertExchange);
    }

    this.channel.assertQueue(this.name, { durable: true }, errAssertQueue => {
      if (errAssertQueue) {
        return callback(errAssertQueue);
      }

      this.channel.bindQueue(this.name, this.name, '', {}, errBindQueue => {
        if (errBindQueue) {
          return callback(errBindQueue);
        }

        const readStream = new PassThrough({ objectMode: true });

        this.channel.consume(this.name, message => {
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

module.exports = Worker;
