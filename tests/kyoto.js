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
    db.increment('iota', 1, function(err, val) {
      if (err) throw err;
      Assert.equal(1, val);
      done();
    });
  },

  'increment again': function(done) {
    db.increment('iota', 1, function(err, val) {
      if (err) throw err;
      Assert.equal(2, val);
      done();
    });
  },

  'increment double': function(done) {
    db.incrementDouble('kappa', 0.1, 1.0, function(err, val) {
      if (err) throw err;
      Assert.equal(1.1, val);
      done();
    });
  },

  'increment double again': function(done) {
    db.incrementDouble('kappa', 0.1, 1.0, function(err, val) {
      if (err) throw err;
      Assert.equal(1.2, val);
      done();
    });
  },

  'close': function(done) {
    db.close(function(err) {
      if (err) throw err;
      done();
    });
  },

  'cursor tests': function(done) {
    db = Kyoto.open('+', 'w+', function(err) {
      if (err) throw err;
      load(done, {
        'alpha': '1',
        'apple': '2',
        'api': '3',
        'aardvark': '4',
        'air': '5',
        'active': '6',
        'arrest': '7',
        'allow': '8'
      });
    });
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

function allEqual(done, expect) {
  var all = {};

  db.each(assert, function(val, key) {
    all[key] = val;
  });

  function assert(err) {
    if (err) throw err;
    Assert.deepEqual(expect, all);
    done();
  }
}

function load(done, data) {
  var keys = Object.keys(data);
  next();

  function next(err) {
    if (err || keys.length === 0)
      done(err);
    else {
      var key = keys.shift();
      db.set(key, data[key], next);
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