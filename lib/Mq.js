'use strict';

const events = require('events'),
      util = require('util');

const Publisher = require('./publisher'),
      Worker = require('./worker');

const EventEmitter = events.EventEmitter;

const Mq = function (connection, channel) {
  this.connection = connection;
  this.channel = channel;

  let onClose,
      onError;

  const unsubscribe = () => {
    this.connection.removeListener('close', onClose);
    this.connection.removeListener('error', onError);
    this.channel.removeListener('close', onClose);
    this.channel.removeListener('error', onError);
  };

  onClose = () => {
    unsubscribe();
    this.emit('disconnect');
  };

  onError = err => {
    if (err.code === 'ECONNRESET' || err.code === 'EPIPE') {
      unsubscribe();

      return this.emit('disconnect');
    }
    this.emit('error', err);
  };

  this.connection.on('close', onClose);
  this.connection.on('error', onError);
  this.channel.on('close', onClose);
  this.channel.on('error', onError);
};

util.inherits(Mq, EventEmitter);

Mq.prototype.worker = function (name) {
  if (!name) {
    throw new Error('Name is missing.');
  }

  return new Worker(this.channel, name);
};

Mq.prototype.publisher = function (name) {
  if (!name) {
    throw new Error('Name is missing.');
  }

  return new Publisher(this.channel, name);
};

module.exports = Mq;
