'use strict';

const url = require('url');

const knock = require('knockat');

const env = require('./env');

const waitForRabbitMq = function (callback) {
  const rabbit = url.parse(env.RABBITMQ_URL);

  knock.at(rabbit.hostname, rabbit.port, errKnockAt => {
    if (errKnockAt) {
      return callback(errKnockAt);
    }
    callback(null);
  });
};

module.exports = waitForRabbitMq;
