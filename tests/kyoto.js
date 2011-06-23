var Assert = require('assert'),
    Kyoto = require('../kyoto'),
    db, cursor;

module.exports = {

  'open': function(done) {
    Kyoto.open('+', function(err) {
      if (err) throw err;
      db = this;
      done();
    });
  },

  'set': function(done) {
    db.set('alpha', 'one', function(err) {
      if (err) throw err;
      done();
    });
  },

  'get': function(done) {
    db.get('alpha', function(err, val) {
      if (err) throw err;
      Assert.equal(val, 'one');
      done();
    });
  },

  'set again': function(done) {
    db.set('alpha', 'changed one', function(err) {
      if (err) throw err;
      allEqual(done, { alpha: 'changed one' });
    });
  },

  'add': function(done) {
    db.add('beta', 'two', function(err) {
      if (err) throw err;
      allEqual(done, { alpha: 'changed one', beta: 'two' });
    });
  },

  'add fails': function(done) {
    db.add('beta', 'replaced two', function(err) {
      Assert.ok(err);
      Assert.equal(err.code, Kyoto.DUPREC);
      allEqual(done, { alpha: 'changed one', beta: 'two' });
    });
  },

  'replace': function(done) {
    db.replace('beta', 'replaced two', function(err) {
      if (err) throw err;
      allEqual(done, { alpha: 'changed one', beta: 'replaced two' });
    });
  },

  'replace fails': function(done) {
    db.replace('gamma', 'three', function(err) {
      Assert.ok(err);
      Assert.equal(err.code, Kyoto.NOREC);
      allEqual(done, { alpha: 'changed one', beta: 'replaced two' });
    });
  },

  'get bulk': function(done) {
    db.getBulk(['alpha', 'beta'], function(err, items) {
      if (err) throw err;
      Assert.deepEqual(items, {
        alpha: 'changed one',
        beta: 'replaced two'
      });
      done();
    });
  },

  'remove': function(done) {
    db.remove('alpha', function(err) {
      if (err) throw err;
      allEqual(done, { beta: 'replaced two' });
    });
  },

  'synchronize': function(done) {
    db.synchronize(function(err) {
      if (err) throw err;
      done();
    });
  },

  'first append': function(done) {
    db.append('epsilon', 'prefix', function(err) {
      if (err) throw err;
      isEqual('epsilon', 'prefix', done);
    });
  },

  'second append': function(done) {
    db.append('epsilon', '-suffix', function(err) {
      if (err) throw err;
      isEqual('epsilon', 'prefix-suffix', done);
    });
  },

  'increment': function(done) {
    db.increment('zeta', 1, function(err, val) {
      if (err) throw err;
      Assert.equal(1, val);
      done();
    });
  },

  'increment again': function(done) {
    db.increment('zeta', 1, function(err, val) {
      if (err) throw err;
      Assert.equal(2, val);
      done();
    });
  },

  'increment double': function(done) {
    db.incrementDouble('eta', 0.1, 1.0, function(err, val) {
      if (err) throw err;
      Assert.equal(1.1, val);
      done();
    });
  },

  'increment double again': function(done) {
    db.incrementDouble('eta', 0.1, 1.0, function(err, val) {
      if (err) throw err;
      Assert.equal(1.2, val);
      done();
    });
  },

  'cas': function(done) {
    db.cas('theta', null, 'first', function(err, success) {
      if (err) throw err;
      Assert.ok(success);
      isEqual('theta', 'first', done);
    });
  },

  'cas again': function(done) {
    db.cas('theta', 'first', 'second', function(err, success) {
      if (err) throw err;
      Assert.ok(success);
      isEqual('theta', 'second', done);
    });
  },

  'cas fail': function(done) {
    db.cas('theta', 'first', 'third', function(err, success) {
      if (err) throw err;
      Assert.ok(!success);
      done();
    });
  },

  'cas remove': function(done) {
    db.cas('theta', 'second', null, function(err, success) {
      if (err) throw err;
      Assert.ok(success);
      isEqual('theta', undefined, done);
    });
  },

  'set bulk': function(done) {
    var data = { 'alpha': '1', 'apple': '2', 'api': '3' };

    db.setBulk(data, true, function(err) {
      if (err) throw err;
      db.getBulk(Object.keys(data), verify);
    });

    function verify(err, items) {
      if (err) throw err;
      Assert.deepEqual(data, items);
      done();
    }
  },

  'remove bulk': function(done) {
    var data = { 'alpha': '1', 'apple': '2', 'api': '3' };

    db.removeBulk(['alpha', 'apple'], function(err) {
      if (err) throw err;
      db.getBulk(['alpha', 'apple', 'api'], verify);
    });

    function verify(err, items) {
      if (err) throw err;
      Assert.deepEqual({ 'api': '3' }, items);
      done();
    }
  },

  'clear': function(done) {
    db.clear(function(err) {
      if (err) throw err;
      allEqual(done, {});
    });
  },

  'cursor tests': function(done) {
    db.setBulk({
      'alpha': '1',
      'apple': '2',
      'api': '3',
      'aardvark': '4',
      'air': '5',
      'active': '6',
      'arrest': '7',
      'allow': '8'
    }, done);
  },

  'cursor jump': function(done) {
    (cursor = db.cursor()).jump(function(err) {
      if (err) throw err;
      done();
    });
  },

  'cursor get': function(done) {
    cursor.get(function(err, val, key) {
      if (err) throw err;
      Assert.equal(key, 'aardvark');
      Assert.equal(val, '4');
      getStep();
    });

    function getStep() {
      cursor.get(true, function(err, val, key) {
        if (err) throw err;
        Assert.equal(key, 'aardvark');
        getAgain();
      });
    }

    function getAgain() {
      cursor.get(function(err, val, key) {
        if (err) throw err;
        Assert.equal(key, 'active');
        Assert.equal(val, '6');
        done();
      });
    }
  },

  'cursor get key': function(done) {
    cursor.jump(function(err) {
      if (err) throw err;
      cursor.getKey(gotFirst);
    });

    function gotFirst(err, key) {
      if (err) throw err;
      Assert.equal(key, 'aardvark');
      cursor.getKey(true, gotAgain);
    }

    function gotAgain(err, key) {
      if (err) throw err;
      Assert.equal(key, 'aardvark');
      cursor.getKey(gotStep);
    }

    function gotStep(err, key) {
      if (err) throw err;
      Assert.equal(key, 'active');
      done();
    }
  },

  'cursor get value': function(done) {
    cursor.jump(function(err) {
      if (err) throw err;
      cursor.getValue(gotFirst);
    });

    function gotFirst(err, val) {
      if (err) throw err;
      Assert.equal(val, '4');
      cursor.getValue(true, gotAgain);
    }

    function gotAgain(err, val) {
      if (err) throw err;
      Assert.equal(val, '4');
      cursor.getValue(gotStep);
    }

    function gotStep(err, val) {
      if (err) throw err;
      Assert.equal(val, '6');
      done();
    }
  },

  'cursor jump back': function(done) {
    cursor.jumpBack(function(err) {
      if (err) throw err;
      cursor.get(true, lastItem);
    });

    function lastItem(err, val, key) {
      if (err) throw err;
      Assert.equal(key, 'arrest');
      Assert.equal(val, '7');
      cursor.get(emptyItem);
    }

    function emptyItem(err, val, key) {
      if (err) throw err;
      Assert.equal(val, undefined);
      done();
    }
  },

  'cursor jump to': function(done) {
    cursor.jump('ap', function(err) {
      if (err) throw err;
      cursor.getKey(getFirst);
    });

    function getFirst(err, key) {
      if (err) throw err;
      Assert.equal(key, 'api');
      cursor.step(function(err) {
        if (err) throw err;
        cursor.getKey(getSecond);
      });
    }

    function getSecond(err, key) {
      if (err) throw err;
      Assert.equal(key, 'apple');
      done();
    }
  },

  'cursor jump back to': function(done) {
    cursor.jumpBack('alz', function(err) {
      if (err) throw err;
      cursor.getKey(getFirst);
    });

    function getFirst(err, key) {
      if (err) throw err;
      Assert.equal(key, 'alpha');
      cursor.stepBack(function(err) {
        if (err) throw err;
        cursor.getKey(getSecond);
      });
    }

    function getSecond(err, key) {
      if (err) throw err;
      Assert.equal(key, 'allow');
      done();
    }
  },

  'cursor set value': function(done) {
    (cursor = db.cursor()).jump('alpha', function(err) {
      if (err) throw err;
      cursor.setValue('alpha-prime', verify);
    });

    function verify(err) {
      if (err) throw err;
      db.get('alpha', function(err, val) {
        if (err) throw err;
        Assert.equal('alpha-prime', val);
        done();
      });
    }
  },

  'cursor remove': function(done) {
    (cursor = db.cursor()).jump('alpha', function(err) {
      if (err) throw err;
      cursor.remove(verify);
    });

    function verify(err) {
      if (err) throw err;
      db.get('alpha', function(err, val) {
        if (err) throw err;
        Assert.equal(undefined, val);
        done();
      });
    }
  },

  'match prefix': function(done) {
    db.matchPrefix('ap', function(err, keys) {
      if (err) throw err;
      Assert.deepEqual(['api', 'apple'], keys);
      done();
    });
  },

  'match regex': function(done) {
    db.matchRegex(/(\w)\1/, function(err, keys) {
      if (err) throw err;
      Assert.deepEqual(['aardvark', 'allow', 'apple', 'arrest'], keys);
      done();
    });
  },

  'snapshot': function(done) {
    var data = { a: '1', b: '2' },
        dest = '/tmp/kyoto-test.snapshot';

    freshDB('+', data, function(err, db1) {
      if (err) throw err;
      db1.dumpSnapshot(dest, load);
    });

    function load(err) {
      if (err) throw err;
      freshDB('+', {}, function(err, db2) {
        if (err) throw err;
        db2.loadSnapshot(dest, compare);
      });
    }

    function compare(err) {
      if (err) throw err;
      allEqual(done, this, data);
    }
  },

  'copy': function(done) {
    var data = { a: '1', b: '2' },
        dest = '/tmp/kyoto-test-copy.kct';

    freshDB('/tmp/kyoto-test-src.kct', data, function(err, db1) {
      if (err) throw err;
      db1.copy(dest, open);
    });

    function open(err) {
      if (err) throw err;
      Kyoto.open(dest, verify);
    }

    function verify(err) {
      if (err) throw err;
      allEqual(done, this, data);
    }
  },

  'count': function(done) {
    freshDB('+', { a: '1', b: '2' }, function(err, store) {
      if (err) throw err;
      store.count(verify);
    });

    function verify(err, total) {
      if (err) throw err;
      Assert.equal(2, total);
      done();
    }
  },

  'size': function(done) {
    db.size(function(err, total) {
      if (err) throw err;
      Assert.ok(total > 0);
      done();
    });
  },

  'status': function(done) {
    db.status(function(err, info) {
      Assert.deepEqual(['count', 'path', 'realtype', 'size', 'type'], Object.keys(info));
      done();
    });
  },

  'close': function(done) {
    db.close(function(err) {
      if (err) throw err;
      done();
    });
  }

};


// ## Helpers ##

function isEqual(key, expect, done) {
  db.get(key, function(err, val) {
    if (err) throw err;
    Assert.equal(expect, val);
    done();
  });
}

function allEqual(done, store, expect) {
  var all = {};

  if (!(store instanceof Kyoto.KyotoDB)) {
    expect = store;
    store = db;
  }

  store.each(assert, function(val, key) {
    all[key] = val;
  });

  function assert(err) {
    if (err) throw err;
    Assert.deepEqual(expect, all);
    done();
  }
}

function load(done, store, data) {
  if (!(store instanceof Kyoto.KyotoDB)) {
    data = store;
    store = db;
  }

  var keys = Object.keys(data);
  next();

  function next(err) {
    if (err || keys.length === 0)
      done(err);
    else {
      var key = keys.shift();
      store.set(key, data[key], next);
    }
  }
}

function showEach(done) {
  cursor.get(true, function(err, val, key) {
    if (err || !key)
      done(err);
    else {
      console.log('show:', key, val);
      showEach(done);
    }
  });
}

function freshDB(path, data, next) {
  var store = Kyoto.open(path, 'w+', function(err) {
    err ? next(err) : load(done, store, data);
  });

  function done(err) {
    next(err, store);
  }
}