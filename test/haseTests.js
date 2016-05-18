'use strict';

const EventEmitter = require('events').EventEmitter,
    Readable = require('stream').Readable,
    Writable = require('stream').Writable;

const assert = require('assertthat'),
    uuid = require('uuidv4');

const hase = require('../lib/hase');

const rabbitUrl = require('./config.json').rabbitUrl;

suite('hase', function () {
  test('is an object.', function (done) {
    assert.that(hase).is.ofType('object');
    done();
  });

  suite('connect', function () {
    test('is a function.', function (done) {
      assert.that(hase.connect).is.ofType('function');
      done();
    });

    test('throws an error if the url is missing.', function (done) {
      assert.that(() => {
        hase.connect();
      }).is.throwing('Url is missing.');
      done();
    });

    test('throws an error if the callback is missing.', function (done) {
      assert.that(() => {
        hase.connect('amqp://...');
      }).is.throwing('Callback is missing.');
      done();
    });

    test('returns an error if it can not connect using the given url.', function (done) {
      hase.connect('amqp://admin:admin@localhost:12345', err => {
        assert.that(err).is.not.null();
        assert.that(err.message).is.equalTo('Could not connect to amqp://admin:admin@localhost:12345.');
        done();
      });
    });

    test('returns a reference to the message queue.', function (done) {
      hase.connect(rabbitUrl, (err, mq) => {
        assert.that(err).is.null();
        assert.that(mq).is.ofType('object');
        done();
      });
    });

    suite('mq', function () {
      let mq;

      suiteSetup(function (done) {
        hase.connect(rabbitUrl, (err, _mq) => {
          mq = _mq;
          done(err);
        });
      });

      test('is an event emitter.', function (done) {
        assert.that(mq instanceof EventEmitter).is.true();
        done();
      });

      suite('worker', function () {
        test('is a function.', function (done) {
          assert.that(mq.worker).is.ofType('function');
          done();
        });

        test('throws an error if name is missing.', function (done) {
          assert.that(() => {
            mq.worker();
          }).is.throwing('Name is missing.');
          done();
        });

        test('returns an object.', function (done) {
          assert.that(mq.worker(uuid())).is.ofType('object');
          done();
        });

        suite('createWriteStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.worker(uuid()).createWriteStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(() => {
              mq.worker(uuid()).createWriteStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a writable stream.', function (done) {
            mq.worker(uuid()).createWriteStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Writable).is.true();
              done();
            });
          });

          suite('writable stream', function () {
            test('does not throw an error when writing.', function (done) {
              mq.worker(uuid()).createWriteStream((err, stream) => {
                assert.that(err).is.null();
                assert.that(() => {
                  stream.write({ foo: 'bar' });
                }).is.not.throwing();
                done();
              });
            });
          });
        });

        suite('createReadStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.worker(uuid()).createReadStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(() => {
              mq.worker(uuid()).createReadStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a readable stream.', function (done) {
            mq.worker(uuid()).createReadStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Readable).is.true();
              done();
            });
          });

          suite('readable stream', function () {
            test('returns a single message.', function (done) {
              const name = uuid();

              mq.worker(name).createWriteStream((err, stream) => {
                assert.that(err).is.null();
                stream.write({ foo: 'bar' });
              });

              mq.worker(name).createReadStream((err, stream) => {
                assert.that(err).is.null();
                stream.once('data', message => {
                  assert.that(message.payload).is.equalTo({ foo: 'bar' });
                  assert.that(message.next).is.ofType('function');
                  assert.that(message.discard).is.ofType('function');
                  assert.that(message.defer).is.ofType('function');
                  message.next();
                  done();
                });
              });
            });

            test('returns multiple messages.', function (done) {
              const name = uuid();

              mq.worker(name).createWriteStream((err, stream) => {
                assert.that(err).is.null();
                stream.write({ foo: 'bar' });
                stream.write({ foo: 'baz' });
              });

              mq.worker(name).createReadStream((err, stream) => {
                assert.that(err).is.null();

                let counter = 0;

                stream.on('data', message => {
                  counter++;
                  /* eslint-disable default-case */
                  switch (counter) {
                  /* eslint-enable default-case */
                    case 1:
                      assert.that(message.payload).is.equalTo({ foo: 'bar' });
                      message.next();
                      break;
                    case 2:
                      assert.that(message.payload).is.equalTo({ foo: 'baz' });
                      message.next();
                      done();
                      break;
                  }
                });
              });
            });

            test('returns multiple messages in the correct order.', function (done) {
              const name = uuid();

              mq.worker(name).createReadStream((errCreateReadStream, readStream) => {
                assert.that(errCreateReadStream).is.null();

                let barFinished = false;

                readStream.on('data', message => {
                  if (message.payload.foo === 'bar') {
                    setTimeout(() => {
                      barFinished = true;
                      message.next();
                    }, 0.5 * 1000);
                  }

                  if (message.payload.foo === 'baz') {
                    assert.that(barFinished).is.true();
                    message.next();
                    done();
                  }
                });

                mq.worker(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });

            suite('discard', function () {
              test('throws away a received message.', function (done) {
                const name = uuid();

                mq.worker(name).createWriteStream((err, stream) => {
                  assert.that(err).is.null();
                  stream.write({ foo: 'bar' });
                  stream.write({ foo: 'baz' });
                });

                mq.worker(name).createReadStream((err, stream) => {
                  assert.that(err).is.null();

                  let counter = 0;

                  stream.on('data', message => {
                    counter++;
                    /* eslint-disable default-case */
                    switch (counter) {
                    /* eslint-enable default-case */
                      case 1:
                        assert.that(message.payload).is.equalTo({ foo: 'bar' });
                        message.discard();
                        break;
                      case 2:
                        assert.that(message.payload).is.equalTo({ foo: 'baz' });
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
                const name = uuid();

                mq.worker(name).createWriteStream((err, stream) => {
                  assert.that(err).is.null();
                  stream.write({ foo: 'bar' });
                });

                mq.worker(name).createReadStream((err, stream) => {
                  assert.that(err).is.null();

                  let counter = 0;

                  stream.on('data', message => {
                    counter++;
                    /* eslint-disable default-case */
                    switch (counter) {
                    /* eslint-enable default-case */
                      case 1:
                        assert.that(message.payload).is.equalTo({ foo: 'bar' });
                        message.defer();
                        break;
                      case 2:
                        assert.that(message.payload).is.equalTo({ foo: 'bar' });
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
          assert.that(mq.publisher).is.ofType('function');
          done();
        });

        test('throws an error if name is missing.', function (done) {
          assert.that(() => {
            mq.publisher();
          }).is.throwing('Name is missing.');
          done();
        });

        test('returns an object.', function (done) {
          assert.that(mq.publisher(uuid())).is.ofType('object');
          done();
        });

        suite('createWriteStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.publisher(uuid()).createWriteStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(() => {
              mq.publisher(uuid()).createWriteStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a writable stream.', function (done) {
            mq.publisher(uuid()).createWriteStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Writable).is.true();
              done();
            });
          });

          suite('writable stream', function () {
            test('does not throw an error when writing.', function (done) {
              mq.publisher(uuid()).createWriteStream((err, stream) => {
                assert.that(err).is.null();
                assert.that(() => {
                  stream.write({ foo: 'bar' });
                }).is.not.throwing();
                done();
              });
            });
          });
        });

        suite('createReadStream', function () {
          test('is a function.', function (done) {
            assert.that(mq.publisher(uuid()).createReadStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', function (done) {
            assert.that(() => {
              mq.publisher(uuid()).createReadStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a readable stream.', function (done) {
            mq.publisher(uuid()).createReadStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Readable).is.true();
              done();
            });
          });

          suite('readable stream', function () {
            test('returns a single message.', function (done) {
              const name = uuid();

              mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                assert.that(errCreateReadStream).is.null();
                readStream.once('data', message => {
                  assert.that(message.payload).is.equalTo({ foo: 'bar' });
                  message.next();
                  done();
                });

                mq.publisher(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                });
              });
            });

            test('returns multiple messages.', function (done) {
              const name = uuid();

              mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                assert.that(errCreateReadStream).is.null();

                let counter = 0;

                readStream.on('data', message => {
                  counter++;
                  /* eslint-disable default-case */
                  switch (counter) {
                  /* eslint-enable default-case */
                    case 1:
                      assert.that(message.payload).is.equalTo({ foo: 'bar' });
                      message.next();
                      break;
                    case 2:
                      assert.that(message.payload).is.equalTo({ foo: 'baz' });
                      message.next();
                      done();
                      break;
                  }
                });

                mq.publisher(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });

            test('returns multiple messages in the correct order.', function (done) {
              const name = uuid();

              mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                assert.that(errCreateReadStream).is.null();

                let barFinished = false;

                readStream.on('data', message => {
                  if (message.payload.foo === 'bar') {
                    setTimeout(() => {
                      barFinished = true;
                      message.next();
                    }, 0.5 * 1000);
                  }

                  if (message.payload.foo === 'baz') {
                    assert.that(barFinished).is.true();
                    message.next();
                    done();
                  }
                });

                mq.publisher(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });

            suite('discard', function () {
              test('throws away a received message.', function (done) {
                const name = uuid();

                mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                  assert.that(errCreateReadStream).is.null();

                  let counter = 0;

                  readStream.on('data', message => {
                    counter++;
                    /* eslint-disable default-case */
                    switch (counter) {
                    /* eslint-enable default-case */
                      case 1:
                        assert.that(message.payload).is.equalTo({ foo: 'bar' });
                        message.discard();
                        break;
                      case 2:
                        assert.that(message.payload).is.equalTo({ foo: 'baz' });
                        message.next();
                        done();
                        break;
                    }
                  });

                  mq.publisher(name).createWriteStream((err, writeStream) => {
                    assert.that(err).is.null();
                    writeStream.write({ foo: 'bar' });
                    writeStream.write({ foo: 'baz' });
                  });
                });
              });
            });

            suite('defer', function () {
              test('requeues a received message.', function (done) {
                const name = uuid();

                mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                  assert.that(errCreateReadStream).is.null();

                  let counter = 0;

                  readStream.on('data', message => {
                    counter++;
                    /* eslint-disable default-case */
                    switch (counter) {
                    /* eslint-enable default-case */
                      case 1:
                        assert.that(message.payload).is.equalTo({ foo: 'bar' });
                        message.defer();
                        break;
                      case 2:
                        assert.that(message.payload).is.equalTo({ foo: 'bar' });
                        message.next();
                        done();
                        break;
                    }
                  });

                  mq.publisher(name).createWriteStream((err, writeStream) => {
                    assert.that(err).is.null();
                    writeStream.write({ foo: 'bar' });
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
