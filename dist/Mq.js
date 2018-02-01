'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var Publisher = require('./publisher'),
    Worker = require('./worker');

var Mq = function (_EventEmitter) {
  _inherits(Mq, _EventEmitter);

  function Mq(connection, channel) {
    _classCallCheck(this, Mq);

    var _this = _possibleConstructorReturn(this, (Mq.__proto__ || Object.getPrototypeOf(Mq)).call(this));

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

  _createClass(Mq, [{
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