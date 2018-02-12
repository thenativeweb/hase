'use strict';

var _stringify = require('babel-runtime/core-js/json/stringify');

var _stringify2 = _interopRequireDefault(_stringify);

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

var _require = require('stream'),
    Writable = _require.Writable;

var WriteStream = function (_Writable) {
  (0, _inherits3.default)(WriteStream, _Writable);

  function WriteStream(channel, name) {
    (0, _classCallCheck3.default)(this, WriteStream);

    var _this = (0, _possibleConstructorReturn3.default)(this, (WriteStream.__proto__ || (0, _getPrototypeOf2.default)(WriteStream)).call(this, { objectMode: true }));

    _this.channel = channel;
    _this.name = name;
    return _this;
  }

  (0, _createClass3.default)(WriteStream, [{
    key: '_write',
    value: function _write(chunk, encoding, callback) {
      this.channel.publish(this.name, '', Buffer.from((0, _stringify2.default)(chunk), 'utf8'), {
        persistent: true
      });
      callback(null);
    }
  }]);
  return WriteStream;
}(Writable);

module.exports = WriteStream;