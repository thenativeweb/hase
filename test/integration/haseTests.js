'use strict';

const EventEmitter = require('events').EventEmitter,
      Readable = require('stream').Readable,
      Writable = require('stream').Writable;

const assert = require('assertthat'),
      shell = require('shelljs'),
      uuid = require('uuidv4');

const env = require('../helpers/env'),
      hase = require('../../lib/hase'),
      waitForRabbitMq = require('../helpers/waitForRabbitMq');

suite('hase', () => {
  test('is an object.', done => {
    assert.that(hase).is.ofType('object');
    done();
  });

  suite('connect', () => {
    test('is a function.', done => {
      assert.that(hase.connect).is.ofType('function');
      done();
    });

    test('throws an error if the url is missing.', done => {
      assert.that(() => {
        hase.connect();
      }).is.throwing('Url is missing.');
      done();
    });

    test('throws an error if the callback is missing.', done => {
      assert.that(() => {
        hase.connect('amqp://...');
      }).is.throwing('Callback is missing.');
      done();
    });

    test('returns an error if it can not connect using the given url.', done => {
      hase.connect('amqp://admin:admin@localhost:12345', err => {
        assert.that(err).is.not.null();
        assert.that(err.message).is.equalTo('Could not connect to amqp://admin:admin@localhost:12345.');
        done();
      });
    });

    test('returns a reference to the message queue.', done => {
      hase.connect(env.RABBITMQ_URL, (err, mq) => {
        assert.that(err).is.null();
        assert.that(mq).is.ofType('object');
        done();
      });
    });

    suite('mq', () => {
      let mq;

      setup(done => {
        hase.connect(env.RABBITMQ_URL, (err, _mq) => {
          mq = _mq;
          done(err);
        });
      });

      test('is an event emitter.', done => {
        assert.that(mq instanceof EventEmitter).is.true();
        done();
      });

      test('emits a disconnect event when the connection to RabbitMQ gets lost.', function (done) {
        this.timeout(5 * 1000);

        mq.once('disconnect', () => {
          shell.exec('docker start rabbitmq', exitCode => {
            assert.that(exitCode).is.equalTo(0);
            waitForRabbitMq(done);
          });
        });

        shell.exec('docker kill rabbitmq');
      });

      suite('worker', () => {
        test('is a function.', done => {
          assert.that(mq.worker).is.ofType('function');
          done();
        });

        test('throws an error if name is missing.', done => {
          assert.that(() => {
            mq.worker();
          }).is.throwing('Name is missing.');
          done();
        });

        test('returns an object.', done => {
          assert.that(mq.worker(uuid())).is.ofType('object');
          done();
        });

        suite('createWriteStream', () => {
          test('is a function.', done => {
            assert.that(mq.worker(uuid()).createWriteStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', done => {
            assert.that(() => {
              mq.worker(uuid()).createWriteStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a writable stream.', done => {
            mq.worker(uuid()).createWriteStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Writable).is.true();
              done();
            });
          });

          suite('writable stream', () => {
            test('does not throw an error when writing.', done => {
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

        suite('createReadStream', () => {
          test('is a function.', done => {
            assert.that(mq.worker(uuid()).createReadStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', done => {
            assert.that(() => {
              mq.worker(uuid()).createReadStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a readable stream.', done => {
            mq.worker(uuid()).createReadStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Readable).is.true();
              done();
            });
          });

          suite('readable stream', () => {
            test('returns a single message.', done => {
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

            test('returns multiple messages.', done => {
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
                  counter += 1;
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

                      return done();
                  }
                });
              });
            });

            test('returns multiple messages in the correct order.', done => {
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

                    return done();
                  }
                });

                mq.worker(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });

            suite('discard', () => {
              test('throws away a received message.', done => {
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
                    counter += 1;
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

                        return done();
                    }
                  });
                });
              });
            });

            suite('defer', () => {
              test('requeues a received message.', done => {
                const name = uuid();

                mq.worker(name).createWriteStream((err, stream) => {
                  assert.that(err).is.null();
                  stream.write({ foo: 'bar' });
                });

                mq.worker(name).createReadStream((err, stream) => {
                  assert.that(err).is.null();

                  let counter = 0;

                  stream.on('data', message => {
                    counter += 1;
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

                        return done();
                    }
                  });
                });
              });
            });
          });
        });
      });

      suite('publisher', () => {
        test('is a function.', done => {
          assert.that(mq.publisher).is.ofType('function');
          done();
        });

        test('throws an error if name is missing.', done => {
          assert.that(() => {
            mq.publisher();
          }).is.throwing('Name is missing.');
          done();
        });

        test('returns an object.', done => {
          assert.that(mq.publisher(uuid())).is.ofType('object');
          done();
        });

        suite('createWriteStream', () => {
          test('is a function.', done => {
            assert.that(mq.publisher(uuid()).createWriteStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', done => {
            assert.that(() => {
              mq.publisher(uuid()).createWriteStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a writable stream.', done => {
            mq.publisher(uuid()).createWriteStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Writable).is.true();
              done();
            });
          });

          suite('writable stream', () => {
            test('does not throw an error when writing.', done => {
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

        suite('createReadStream', () => {
          test('is a function.', done => {
            assert.that(mq.publisher(uuid()).createReadStream).is.ofType('function');
            done();
          });

          test('throws an error if callback is missing.', done => {
            assert.that(() => {
              mq.publisher(uuid()).createReadStream();
            }).is.throwing('Callback is missing.');
            done();
          });

          test('returns a readable stream.', done => {
            mq.publisher(uuid()).createReadStream((err, stream) => {
              assert.that(err).is.null();
              assert.that(stream instanceof Readable).is.true();
              done();
            });
          });

          suite('readable stream', () => {
            test('returns a single message.', done => {
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

            test('returns multiple messages.', done => {
              const name = uuid();

              mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                assert.that(errCreateReadStream).is.null();

                let counter = 0;

                readStream.on('data', message => {
                  counter += 1;
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

                      return done();
                  }
                });

                mq.publisher(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });

            test('returns multiple messages in the correct order.', done => {
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

                    return done();
                  }
                });

                mq.publisher(name).createWriteStream((err, writeStream) => {
                  assert.that(err).is.null();
                  writeStream.write({ foo: 'bar' });
                  writeStream.write({ foo: 'baz' });
                });
              });
            });

            suite('discard', () => {
              test('throws away a received message.', done => {
                const name = uuid();

                mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                  assert.that(errCreateReadStream).is.null();

                  let counter = 0;

                  readStream.on('data', message => {
                    counter += 1;
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

                        return done();
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

            suite('defer', () => {
              test('requeues a received message.', done => {
                const name = uuid();

                mq.publisher(name).createReadStream((errCreateReadStream, readStream) => {
                  assert.that(errCreateReadStream).is.null();

                  let counter = 0;

                  readStream.on('data', message => {
                    counter += 1;
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

                        return done();
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
