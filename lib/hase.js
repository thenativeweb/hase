'use strict';

const util = require('util');

const amqp = require('amqplib/callback_api');

const Mq = require('./Mq');

const hase = {};

hase.connect = function (url, callback) {
  if (!url) {
    throw new Error('Url is missing.');
  }

  if (!callback) {
    throw new Error('Callback is missing.');
  }

  amqp.connect(url, {}, (errConnect, connection) => {
    if (errConnect) {
      return callback(new Error(util.format('Could not connect to %s.', url)));
    }

    connection.createChannel((err, channel) => {
      if (err) {
        return callback(err);
      }

      channel.prefetch(1);

      callback(null, new Mq(connection, channel));
    });
  });
};

module.exports = hase;
