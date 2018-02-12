'use strict';

var _regenerator = require('babel-runtime/regenerator');

var _regenerator2 = _interopRequireDefault(_regenerator);

var _asyncToGenerator2 = require('babel-runtime/helpers/asyncToGenerator');

var _asyncToGenerator3 = _interopRequireDefault(_asyncToGenerator2);

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var _require = require('stream'),
    PassThrough = _require.PassThrough;

var WriteStream = require('./WriteStream');

var Worker = function () {
  function Worker(channel, name) {
    (0, _classCallCheck3.default)(this, Worker);

    this.channel = channel;
    this.name = name;
  }

  (0, _createClass3.default)(Worker, [{
    key: 'createWriteStream',
    value: function () {
      var _ref = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee() {
        return _regenerator2.default.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _context.next = 2;
                return this.channel.assertExchange(this.name, 'direct', { durable: true });

              case 2:
                _context.next = 4;
                return this.channel.assertQueue(this.name, { durable: true });

              case 4:
                _context.next = 6;
                return this.channel.bindQueue(this.name, this.name, '', {});

              case 6:
                return _context.abrupt('return', new WriteStream(this.channel, this.name));

              case 7:
              case 'end':
                return _context.stop();
            }
          }
        }, _callee, this);
      }));

      function createWriteStream() {
        return _ref.apply(this, arguments);
      }

      return createWriteStream;
    }()
  }, {
    key: 'createReadStream',
    value: function () {
      var _ref2 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee2() {
        var _this = this;

        var readStream;
        return _regenerator2.default.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                _context2.next = 2;
                return this.channel.assertExchange(this.name, 'direct', { durable: true });

              case 2:
                _context2.next = 4;
                return this.channel.assertQueue(this.name, { durable: true });

              case 4:
                _context2.next = 6;
                return this.channel.bindQueue(this.name, this.name, '', {});

              case 6:
                readStream = new PassThrough({ objectMode: true });
                _context2.next = 9;
                return this.channel.consume(this.name, function (message) {
                  var parsedMessage = {};

                  parsedMessage.payload = JSON.parse(message.content.toString('utf8'));

                  parsedMessage.next = function () {
                    _this.channel.ack(message);
                  };

                  parsedMessage.discard = function () {
                    _this.channel.nack(message, false, false);
                  };

                  parsedMessage.defer = function () {
                    _this.channel.nack(message, false, true);
                  };

                  readStream.write(parsedMessage);
                }, {});

              case 9:
                return _context2.abrupt('return', readStream);

              case 10:
              case 'end':
                return _context2.stop();
            }
          }
        }, _callee2, this);
      }));

      function createReadStream() {
        return _ref2.apply(this, arguments);
      }

      return createReadStream;
    }()
  }]);
  return Worker;
}();

module.exports = Worker;