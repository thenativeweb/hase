'use strict';

var _regenerator = require('babel-runtime/regenerator');

var _regenerator2 = _interopRequireDefault(_regenerator);

var _asyncToGenerator2 = require('babel-runtime/helpers/asyncToGenerator');

var _asyncToGenerator3 = _interopRequireDefault(_asyncToGenerator2);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var amqp = require('amqplib');

var Mq = require('./Mq');

var hase = {};

hase.connect = function () {
  var _ref = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee(url) {
    var connection, channel;
    return _regenerator2.default.wrap(function _callee$(_context) {
      while (1) {
        switch (_context.prev = _context.next) {
          case 0:
            if (url) {
              _context.next = 2;
              break;
            }

            throw new Error('Url is missing.');

          case 2:
            connection = void 0;
            _context.prev = 3;
            _context.next = 6;
            return amqp.connect(url, {});

          case 6:
            connection = _context.sent;
            _context.next = 12;
            break;

          case 9:
            _context.prev = 9;
            _context.t0 = _context['catch'](3);
            throw new Error('Could not connect to ' + url + '.');

          case 12:
            _context.next = 14;
            return connection.createChannel();

          case 14:
            channel = _context.sent;


            channel.prefetch(1);

            return _context.abrupt('return', new Mq(connection, channel));

          case 17:
          case 'end':
            return _context.stop();
        }
      }
    }, _callee, this, [[3, 9]]);
  }));

  return function (_x) {
    return _ref.apply(this, arguments);
  };
}();

module.exports = hase;