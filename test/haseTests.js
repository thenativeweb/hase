'use strict';

var events = require('events'),
    stream = require('stream');

var assert = require('node-assertthat'),
    uuid = require('uuidv4');

var hase = require('../lib/hase');

var EventEmitter = events.EventEmitter,
    Readable = stream.Readable,
    Writable = stream.Writable;

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
      hase.connect('amqp://admin:admin@localhost:5672', function (err, mq) {
        assert.that(err, is.null());
        assert.that(mq, is.ofType('object'));
        done();
      });
    });

    suite('mq', function () {
      var mq;

      suiteSetup(function (done) {
        hase.connect('amqp://admin:admin@localhost:5672', function (err, _mq) {
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
            test('has a next function.', function (done) {
              mq.worker(uuid()).createReadStream(function (err, stream) {
                assert.that(err, is.null());
                assert.that(stream.next, is.ofType('function'));
                done();
              });
            });

            test('has a discard function.', function (done) {
              mq.worker(uuid()).createReadStream(function (err, stream) {
                assert.that(err, is.null());
                assert.that(stream.discard, is.ofType('function'));
                done();
              });
            });

            test('has a defer function.', function (done) {
              mq.worker(uuid()).createReadStream(function (err, stream) {
                assert.that(err, is.null());
                assert.that(stream.defer, is.ofType('function'));
                done();
              });
            });

            test('returns a single message.', function (done) {
              var name = uuid();

              mq.worker(name).createWriteStream(function (err, stream) {
                assert.that(err, is.null());
                stream.write({ foo: 'bar' });
              });

              mq.worker(name).createReadStream(function (err, stream) {
                assert.that(err, is.null());
                stream.once('data', function (message) {
                  assert.that(message, is.equalTo({ foo: 'bar' }));
                  stream.next();
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
                      assert.that(message, is.equalTo({ foo: 'bar' }));
                      stream.next();
                      break;
                    case 2:
                      assert.that(message, is.equalTo({ foo: 'baz' }));
                      stream.next();
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
                        assert.that(message, is.equalTo({ foo: 'bar' }));
                        stream.discard();
                        break;
                      case 2:
                        assert.that(message, is.equalTo({ foo: 'baz' }));
                        stream.next();
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
                        assert.that(message, is.equalTo({ foo: 'bar' }));
                        stream.defer();
                        break;
                      case 2:
                        assert.that(message, is.equalTo({ foo: 'bar' }));
                        stream.next();
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
    });
  });
});
