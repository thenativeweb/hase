'use strict';

var events = require('events'),
    util = require('util');

var Publisher = require('./publisher'),
    Worker = require('./worker');

var EventEmitter = events.EventEmitter;

var Mq = function (connection, channel) {
  var that = this;

  that.connection = connection;
  that.channel = channel;

  that.connection.on('error', function (err) {
    that.emit('error', err);
  });

  that.channel.on('error', function (err) {
    that.emit('error', err);
  });

  that.connection.on('close', function () {
    that.emit('error', new Error('Lost connection.'));
  });

  that.channel.on('close', function () {
    that.emit('error', new Error('Lost connection.'));
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
