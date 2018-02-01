'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _require = require('stream'),
    PassThrough = _require.PassThrough;

var WriteStream = require('./WriteStream');

var Worker = function () {
  function Worker(channel, name) {
    _classCallCheck(this, Worker);

    this.channel = channel;
    this.name = name;
  }

  _createClass(Worker, [{
    key: 'createWriteStream',
    value: function () {
      var _ref = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee() {
        return regeneratorRuntime.wrap(function _callee$(_context) {
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
      var _ref2 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee2() {
        var _this = this;

        var readStream;
        return regeneratorRuntime.wrap(function _callee2$(_context2) {
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