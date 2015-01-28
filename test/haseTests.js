'use strict';

var EventEmitter = require('events').EventEmitter,
    Readable = require('stream').Readable,
    Writable = require('stream').Writable;

var assert = require('assertthat'),
    uuid = require('uuidv4');

var hase = require('../lib/hase');

var rabbitUrl = require('./config.json').rabbitUrl;

suite('hase', function () {
  test('is an object.', function (done) {
    assert.that(hase, is.ofType('object'));
    done();
  });

  suite('connect', function () {
    test('is a function.', function (done) {
      assert.that(hase.connect, is.ofType('function'));
      done();
    });

    test('throws an error if the url is missing.', function (done) {
      assert.that(function () {
        hase.connect();
      }, is.throwing('Url is missing.'));
      done();
    });

    test('throws an error if the callback is missing.', function (done) {
      assert.that(function () {
        hase.connect('amqp://...');
      }, is.throwing('Callback is missing.'));
      done();
    });

    test('returns an error if it can not connect using the given url.', function (done) {
      hase.connect('amqp://admin:admin@localhost:12345', function (err) {
        assert.that(err, is.not.null());
        assert.that(err.message, is.equalTo('Could not connect to amqp://admin:admin@localhost:12345.'));
        done();
      });
    });

    test('returns a reference to the message queue.', function (done) {
      hase.connect(rabbitUrl, function (err, mq) {
        assert.that(err, is.null());
        assert.that(mq, is.ofType('object'));
        done();
      });
    });

    suite('mq', function () {
      var mq;

      suiteSetup(function (done) {
        hase.connect(rabbitUrl, function (err, _mq) {
          mq = _mq;
          done(err);
        });
      });

      test('is an event emitter.', function (done) {
        assert.that(mq instanceof EventEmitter, is.true());
        done();
      });

      suite('worker', function () {
        test('is a function.', function (done) {
          assert.that(mq.worker, is.ofType('function'));
          done();
        });

        test('throws an error if name is missing.', function (done) {
          assert.that(function () {
            mq.worker();
          }, is.throwing('Name is missing.'));
          done();
        });

        test('returns an object.', function (done) {
          assert.that(mq.worker(uuid()), is.ofType('object'));
          done();
        });

        suite('createWriteStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.worker(uuid()).createWriteStream, is.ofType('function'));
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(function () {
              mq.worker(uuid()).createWriteStream();
            }, is.throwing('Callback is missing.'));
            done();
          });

          test('returns a writable stream.', function (done) {
            mq.worker(uuid()).createWriteStream(function (err, stream) {
              assert.that(err, is.null());
              assert.that(stream instanceof Writable, is.true());
              done();
            });
          });

          suite('writable stream', function () {
            test('does not throw an error when writing.', function (done) {
              mq.worker(uuid()).createWriteStream(function (err, stream) {
                assert.that(err, is.null());
                assert.that(function () {
                  stream.write({ foo: 'bar' });
                }, is.not.throwing());
                done();
              });
            });
          });
        });

        suite('createReadStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.worker(uuid()).createReadStream, is.ofType('function'));
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(function () {
              mq.worker(uuid()).createReadStream();
            }, is.throwing('Callback is missing.'));
            done();
          });

          test('returns a readable stream.', function (done) {
            mq.worker(uuid()).createReadStream(function (err, stream) {
              assert.that(err, is.null());
              assert.that(stream instanceof Readable, is.true());
              done();
            });
          });

          suite('readable stream', function () {
            test('returns a single message.', function (done) {
              var name = uuid();

              mq.worker(name).createWriteStream(function (err, stream) {
                assert.that(err, is.null());
                stream.write({ foo: 'bar' });
              });

              mq.worker(name).createReadStream(function (err, stream) {
                assert.that(err, is.null());
                stream.once('data', function (message) {
                  assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                  assert.that(message.next, is.ofType('function'));
                  assert.that(message.discard, is.ofType('function'));
                  assert.that(message.defer, is.ofType('function'));
                  message.next();
                  done();
                });
              });
            });

            test('returns multiple messages.', function (done) {
              var name = uuid();

              mq.worker(name).createWriteStream(function (err, stream) {
                assert.that(err, is.null());
                stream.write({ foo: 'bar' });
                stream.write({ foo: 'baz' });
              });

              mq.worker(name).createReadStream(function (err, stream) {
                var counter;

                assert.that(err, is.null());

                counter = 0;
                stream.on('data', function (message) {
                  counter++;
                  /*eslint-disable default-case*/
                  switch (counter) {
                  /*eslint-enable default-case*/
                    case 1:
                      assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                      message.next();
                      break;
                    case 2:
                      assert.that(message.payload, is.equalTo({ foo: 'baz' }));
                      message.next();
                      done();
                      break;
                  }
                });
              });
            });

            suite('discard', function () {
              test('throws away a received message.', function (done) {
                var name = uuid();

                mq.worker(name).createWriteStream(function (err, stream) {
                  assert.that(err, is.null());
                  stream.write({ foo: 'bar' });
                  stream.write({ foo: 'baz' });
                });

                mq.worker(name).createReadStream(function (err, stream) {
                  var counter;

                  assert.that(err, is.null());

                  counter = 0;
                  stream.on('data', function (message) {
                    counter++;
                    /*eslint-disable default-case*/
                    switch (counter) {
                    /*eslint-enable default-case*/
                      case 1:
                        assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                        message.discard();
                        break;
                      case 2:
                        assert.that(message.payload, is.equalTo({ foo: 'baz' }));
                        message.next();
                        done();
                        break;
                    }
                  });
                });
              });
            });

            suite('defer', function () {
              test('requeues a received message.', function (done) {
                var name = uuid();

                mq.worker(name).createWriteStream(function (err, stream) {
                  assert.that(err, is.null());
                  stream.write({ foo: 'bar' });
                });

                mq.worker(name).createReadStream(function (err, stream) {
                  var counter;

                  assert.that(err, is.null());

                  counter = 0;
                  stream.on('data', function (message) {
                    counter++;
                    /*eslint-disable default-case*/
                    switch (counter) {
                    /*eslint-enable default-case*/
                      case 1:
                        assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                        message.defer();
                        break;
                      case 2:
                        assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                        message.next();
                        done();
                        break;
                    }
                  });
                });
              });
            });
          });
        });
      });

      suite('publisher', function () {
        test('is a function.', function (done) {
          assert.that(mq.publisher, is.ofType('function'));
          done();
        });

        test('throws an error if name is missing.', function (done) {
          assert.that(function () {
            mq.publisher();
          }, is.throwing('Name is missing.'));
          done();
        });

        test('returns an object.', function (done) {
          assert.that(mq.publisher(uuid()), is.ofType('object'));
          done();
        });

        suite('createWriteStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.publisher(uuid()).createWriteStream, is.ofType('function'));
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(function () {
              mq.publisher(uuid()).createWriteStream();
            }, is.throwing('Callback is missing.'));
            done();
          });

          test('returns a writable stream.', function (done) {
            mq.publisher(uuid()).createWriteStream(function (err, stream) {
              assert.that(err, is.null());
              assert.that(stream instanceof Writable, is.true());
              done();
            });
          });

          suite('writable stream', function () {
            test('does not throw an error when writing.', function (done) {
              mq.publisher(uuid()).createWriteStream(function (err, stream) {
                assert.that(err, is.null());
                assert.that(function () {
                  stream.write({ foo: 'bar' });
                }, is.not.throwing());
                done();
              });
            });
          });
        });

        suite('createReadStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.publisher(uuid()).createReadStream, is.ofType('function'));
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(function () {
              mq.publisher(uuid()).createReadStream();
            }, is.throwing('Callback is missing.'));
            done();
          });

          test('returns a readable stream.', function (done) {
            mq.publisher(uuid()).createReadStream(function (err, stream) {
              assert.that(err, is.null());
              assert.that(stream instanceof Readable, is.true());
              done();
            });
          });

          suite('readable stream', function () {
            test('returns a single message.', function (done) {
              var name = uuid();

              mq.publisher(name).createReadStream(function (errCreateReadStream, readStream) {
                assert.that(errCreateReadStream, is.null());
                readStream.once('data', function (message) {
                  assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                  done();
                });

                mq.publisher(name).createWriteStream(function (err, writeStream) {
                  assert.that(err, is.null());
                  writeStream.write({ foo: 'bar' });
                });
              });
            });

            test('returns multiple messages.', function (done) {
              var name = uuid();

              mq.publisher(name).createReadStream(function (errCreateReadStream, readStream) {
                var counter;

                assert.that(errCreateReadStream, is.null());

                counter = 0;
                readStream.on('data', function (message) {
                  counter++;
                  /*eslint-disable default-case*/
                  switch (counter) {
                  /*eslint-enable default-case*/
                    case 1:
                      assert.that(message.payload, is.equalTo({ foo: 'bar' }));
                      break;
                    case 2:
                      assert.that(message.payload, is.equalTo({ foo: 'baz' }));
                      done();
                      break;
                  }
                });

                mq.publisher(name).createWriteStream(function (err, writeStream) {
                  assert.that(err, is.null());
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });
          });
        });
      });
    });
  });
});
