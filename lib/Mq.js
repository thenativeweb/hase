'use strict';

const events = require('events'),
      util = require('util');

const Publisher = require('./publisher'),
      Worker = require('./worker');

const EventEmitter = events.EventEmitter;

const Mq = function (connection, channel) {
  this.connection = connection;
  this.channel = channel;

  this.connection.on('error', err => {
    this.emit('error', err);
  });

  this.channel.on('error', err => {
    this.emit('error', err);
  });

  this.connection.on('close', () => {
    this.emit('error', new Error('Lost connection.'));
  });

  this.channel.on('close', () => {
    this.emit('error', new Error('Lost connection.'));
  });
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
