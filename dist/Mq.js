'use strict';

var _getPrototypeOf = require('babel-runtime/core-js/object/get-prototype-of');

var _getPrototypeOf2 = _interopRequireDefault(_getPrototypeOf);

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

var _possibleConstructorReturn2 = require('babel-runtime/helpers/possibleConstructorReturn');

var _possibleConstructorReturn3 = _interopRequireDefault(_possibleConstructorReturn2);

var _inherits2 = require('babel-runtime/helpers/inherits');

var _inherits3 = _interopRequireDefault(_inherits2);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var Publisher = require('./publisher'),
    Worker = require('./worker');

var Mq = function (_EventEmitter) {
  (0, _inherits3.default)(Mq, _EventEmitter);

  function Mq(connection, channel) {
    (0, _classCallCheck3.default)(this, Mq);

    var _this = (0, _possibleConstructorReturn3.default)(this, (Mq.__proto__ || (0, _getPrototypeOf2.default)(Mq)).call(this));

    _this.connection = connection;
    _this.channel = channel;

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

    _this.connection.on('close', onClose);
    _this.connection.on('error', onError);
    _this.channel.on('close', onClose);
    _this.channel.on('error', onError);
    return _this;
  }

  (0, _createClass3.default)(Mq, [{
    key: 'worker',
    value: function worker(name) {
      if (!name) {
        throw new Error('Name is missing.');
      }

      return new Worker(this.channel, name);
    }
  }, {
    key: 'publisher',
    value: function publisher(name) {
      if (!name) {
        throw new Error('Name is missing.');
      }

      return new Publisher(this.channel, name);
    }
  }]);
  return Mq;
}(EventEmitter);

module.exports = Mq;