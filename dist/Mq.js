'use strict';

var events = require('events'),
    util = require('util');

var Publisher = require('./publisher'),
    Worker = require('./worker');

var EventEmitter = events.EventEmitter;

var Mq = function Mq(connection, channel) {
  var _this = this;

  this.connection = connection;
  this.channel = channel;

  var onClose = void 0,
      onError = void 0;

  var unsubscribe = function unsubscribe() {
    _this.connection.removeListener('close', onClose);
    _this.connection.removeListener('error', onError);
    _this.channel.removeListener('close', onClose);
    _this.channel.removeListener('error', onError);
  };

  onClose = function onClose() {
    unsubscribe();
    _this.emit('disconnect');
  };

  onError = function onError() {
    unsubscribe();
    _this.emit('disconnect');
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