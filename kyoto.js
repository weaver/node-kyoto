// # kyoto.js #
//
// A light wrapper around C++ bindings that adds some friendly
// methods, sanity checks, and consistent error dispatching. It's
// possible to do all of this in the bindings, but it's easier this
// way.

var K = require('./build/_kyoto'),
    LOGIC = K.PolyDB.LOGIC,
    NOREC = K.PolyDB.NOREC;

exports.open = open;
exports.KyotoDB = KyotoDB;
exports.Cursor = Cursor;

// Re-export all constants.
for (var name in K.PolyDB) {
  if (/^[A-Z]+$/.test(name))
    exports[name] = K.PolyDB[name];
}


// ## KyotoDB ##

// A KyotoDB can open any type of Kyoto Cabinet database. Use the
// filename extension to indicate the type.
//
// In this example, a database is opened in read/write mode and
// created if it doesn't exist. An item is stored in the database and
// then retrieved:
//
//     var db = open('/tmp/data.kch', 'a+', populate);
//
//     function populate(err) {
//       if (err) throw err;
//       db.set('my', 'value', query);
//     }
//
//     function query(err) {
//       if (err) throw err;
//       db.get('my', function(err, value) {
//         if (err) throw err;
//         console.log('My value is', value);
//         done();
//       });
//     }
//
//     function done() {
//       db.close(function(err) {
//         if (err) throw err;
//       });
//     }

// Create a new KyotoDB instance and open it.
function open(path, mode, next) {
  return (new KyotoDB()).open(path, mode, next);
}

// Construct a new database hande.
function KyotoDB() {
  this.db = null;
}

// Open a database.
//
// The type of database is determined by the extension of `path`:
//
//   + `.kch` - file hash
//   + `.kct` - file tree
//   + `.kcd` - directory hash
//   + `.kcf` - directory tree
//
// Memory-only databases can be opened using these special `path`
// values:
//
//   + `-` - memory hash
//   + `+` - memory tree
//
// The `path` can also have tuning parameters appended to
// it. Parameters should be given in a `#key1=value1#key2=value2...`
// format. Refer to Kyoto Cabinet's `PolyDB::open()` documentation.
//
// The mode can be a number (e.g. `OWRITER | OCREATE | OTRUNCATE`) or
// one of:
//
//   + `r`  - read only  (file must exist)
//   + `r+` - read/write (file must exist)
//   + `w+` - read/write (always make a new file)
//   + `a+` - read/write (make a new file if it doesn't exist)
//
// open(path, mode='r', next)
//
//   + path - String database file.
//   + mode - String open mode (optional, default: 'r' or 'w+' if memory-only)
//   + next - Function(Error) callback
//
// Returns self.
KyotoDB.prototype.open = function(path, mode, next) {
  var self = this;

  if (this.db !== null) {
    next.call(this, null);
    return this;
  }

  if (typeof mode == 'function') {
    next = mode;
    mode = (path == '-' || path == '+') ? 'w+' : 'r';
  }

  if (!next)
    next = noop;

  var omode = parseMode(mode);
  if (!omode) {
    next.call(this, new Error('Badly formatted mode: `' + mode + '`.'));
    return this;
  }

  var db = new K.PolyDB();
  db.open(path, omode, function(err) {
    if (err)
      next.call(self, err);
    else {
      self.db = db;
      next.call(self, null);
    }
  });

  return this;
};

// Close a database
//
// + next - Function(Error) callback
//
// Returns self.
KyotoDB.prototype.close = function(next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null) {
    next.call(this, null);
    return this;
  }

  this.db.close(function(err) {
    if (err)
      next.call(self, err);
    else {
      self.db = null;
      next.call(self, null);
    }
  });

  return this;
};

KyotoDB.prototype.closeSync = function() {
  if (this.db) {
    this.db.closeSync();
    this.db = null;
  }
  return this;
};

// Clear all items from the database.
//
// + next - Function(Error) callback
//
// Returns self
KyotoDB.prototype.clear = function(next) {
  var self = this;

  if (this.db === null)
    next.call(this, new Error('clear: database is closed.'));
  else
    this.db.clear(next);

  return this;
};

// Get a value from the database.
//
// If the value does not exist, `next` is called with a `null` error
// and an undefined `value`.
//
// + key  - String key
// + next - Function(Error, String value, String key) callback
//
// Returns self.
KyotoDB.prototype.get = function(key, next) {
  var self = this;

  if (this.db === null)
    next.call(this, new Error('get: database is closed.'));
  else
    this.db.get(key, function(err, val) {
      if (err && err.code == NOREC)
        next.call(self, null, undefined, key);
      else if (err)
        next.call(self, err);
      else
        next.call(self, null, val, key);
    });

  return this;
};

// Get a set of values from the database.
//
// Look up each key in the `keys` array; call `next` with a result
// object that maps each key to its value. If the item doesn't exist,
// `key` will not be in the result object.
//
// + keys   - Array of keys
// + atomic - Boolean fetch all keys atomically (optional, default: false)
// + next   - Function(Error, Object items, Array keys)
//
// Returns self
KyotoDB.prototype.getBulk = function(keys, atomic, next) {
  return this._bulk('getBulk', keys, atomic, next);
};

// Find a set of keys that start with the given prefix.
//
// If `max` is given, the list of keys returned will be at most `max`
// elements long. If `max` is `-1` (the default), there is no limit on
// the result set.
//
// + prefix - String keys start with this
// + max    - Integer maximum result set size (optional, default: -1)
// + next   - Function(Error, Array keys)
//
// Returns self
KyotoDB.prototype.matchPrefix = function(prefix, max, next) {
  return this._match('matchPrefix', prefix, max, next);
};

// Find a set of keys that match a given pattern.
//
// If `max` is given, the list of keys returned will be at most `max`
// elements long. If `max` is `-1` (the default), there is no limit on
// the result set.
//
// + regex - String pattern to match keys against
// + max   - Integer maximum result set size (optional, default: -1)
// + next  - Function(Error, Array keys)
//
// Returns self
KyotoDB.prototype.matchRegex = function(pattern, max, next) {
  if (pattern instanceof RegExp)
    pattern = pattern.source;
  return this._match('matchRegex', pattern, max, next);
};

// Append to a value in the database.
//
// + key    - String key
// + suffix - String value
// + next   - Function(Error, String suffix, String key) callback
//
// Returns self.
KyotoDB.prototype.append = function(key, suffix, next) {
  return this._modify('append', key, suffix, next);
};

// Increment an integer value in the database.
//
// + key  - String key
// + num  - Integer number to increment by
// + orig - Integer base number if not already set (optional, default: 0)
// + next - Function(Error, Integer value, String key) callback
//
// Returns self.
KyotoDB.prototype.increment = function(key, num, orig, next) {
  return this._inc('increment', key, num, orig, next);
};

// Increment a double value in the database.
//
// + key  - String key
// + num  - Double number to increment by
// + orig - Double base number if not already set (optional, default: 0.0)
// + next - Function(Error, Double value, String key) callback
//
// Returns self.
KyotoDB.prototype.incrementDouble = function(key, num, orig, next) {
  return this._inc('incrementDouble', key, num, orig, next);
};

// Set a value in the database.
//
// + key   - String key
// + value - String value
// + next  - Function(Error, String value, String key) callback
//
// Returns self.
KyotoDB.prototype.set = function(key, val, next) {
  return this._modify('set', key, val, next);
};

// Add a value to the database.
//
// Set the value for `key` if isn't already bound in the database. If
// `key` is in the database, the original value is kept and an error
// with code `DUPREC` is raised.
//
// + key   - String key
// + value - String value
// + next  - Function(Error, String value, String key) callback
//
// Returns self.
KyotoDB.prototype.add = function(key, val, next) {
  return this._modify('add', key, val, next);
};

// Replace value in the database.
//
// Change the value for `key` in the database. If `key` isn't in the
// database, raise an error with code `NOREC`.
//
// + key   - String key
// + value - String value
// + next  - Function(Error, String value, String key) callback
//
// Returns self.
KyotoDB.prototype.replace = function(key, val, next) {
  return this._modify('replace', key, val, next);
};

// Remove a value from the database.
//
// + key   - String key
// + next  - Function(Error) callback
//
// Returns self.
KyotoDB.prototype.remove = function(key, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error('remove: database is closed.'));
  else
    this.db.remove(key, function(err) {
      next.call(self, err);
    });

  return this;
};

// Compare and swap
//
// + key    - String key
// + ovalue - String old value (or null if it doesn't exist)
// + nvalue - String new value (or null to remove)
// + next   - Function(Error, success, String key) callback
//
// Returns self.
KyotoDB.prototype.cas = function(key, ovalue, nvalue, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error('cas: database is closed.'));
  else
    this.db.cas(key, ovalue, nvalue, function(err, success) {
      next.call(self, err, success, key);
    });

  return this;
};

// Set multiple items at once the database.
//
// Set all key/value pairs in the `items` object; call `next` with the
// number of items written.
//
// + items  - Object of key/value items
// + atomic - Boolean perform atomic operation (optional, default: false)
// + next   - Function(Error, Integer written, Object items)
//
// Returns self.
KyotoDB.prototype.setBulk = function(items, atomic, next) {
  return this._bulk('setBulk', items, atomic, next);
};

// Remove several items from the database.
//
// Upon success, `next` is called with the number of items removed.
//
// + keys   - Array of keys
// + atomic - Boolean perform atomic operation (optional, default: false)
// + next   - Function(Error, Integer removed, Array keys)
//
// Returns self.
KyotoDB.prototype.removeBulk = function(keys, atomic, next) {
  return this._bulk('removeBulk', keys, atomic, next);
};

// Flush everything to disk.
//
// If the optional `hard` parameter is `true`, physically synchronize
// with the underlying device. Otherwise, logically synchronize with
// the file system.
//
// + hard - Boolean physical sync (optional, default: false)
// + next - Function(Error) callback
//
// Returns self.
KyotoDB.prototype.synchronize = function(hard, next) {
  var self = this;

  if (typeof hard == 'function') {
    next = hard;
    hard = undefined;
  }

  hard = (hard === undefined) ? false : hard;
  next = next || noop;

  if (this.db === null)
    next.call(this, new Error('synchronize: database is closed.'));
  else
    this.db.synchronize(hard, function(err) {
      next.call(self, err);
    });

  return this;
};

// Copy the current database file to another file.
//
// + path - String destination
// + next - Function(Error) callback
//
// Returns self
KyotoDB.prototype.copy = function(path, next) {
  return this._snap('copy', path, next);
};

// Create a snapshot of this database, write it to a file.
//
// + path - String destination
// + next - Function(Error) callback
//
// Returns self
KyotoDB.prototype.dumpSnapshot = function(path, next) {
  return this._snap('dumpSnapshot', path, next);
};

// Load a snapshot file into this database.
//
// + path - String snapshot source
// + next - Function(Error) callback
//
// Returns self
KyotoDB.prototype.loadSnapshot = function(path, next) {
  return this._snap('loadSnapshot', path, next);
};

// Find the number of records currently stored.
//
// + next - Function(Error, Integer) callback
//
// Returns self
KyotoDB.prototype.count = function(next) {
  return this._stat('count', next);
};

// Find current size of the database in bytes.
//
// + next - Function(Error, Integer) callback
//
// Returns self
KyotoDB.prototype.size = function(next) {
  return this._stat('size', next);
};

// Retrieve status information about the current database.
//
// + next - Function(Error, Object info) callback
//
// Returns self
KyotoDB.prototype.status = function(next) {
  return this._stat('status', next);
};

// TODO: merge, scan_parallel

// KyotoDB.prototype.merge = function(others, next) {
//   var self = this;

//   next = next || noop;
//   for (var i = 0, l = others.length; i < l; i++) {
//     if (!(others instanceof KyotoDB)) {
//       next.call(this, new Error('merge: requires array of KyotoDB instances.'));
//       return this;
//     }
//   }

//   if (this.db === null)
//     next.call(this, new Error('merge: database is closed.'));
//   else
//     this.db.merge(others, function(err) {
//       next.call(self, err);
//     });

//   return this;
// };

// A low-level helper method. See add() or set().
KyotoDB.prototype._modify = function(method, key, val, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](key, val, function(err) {
      next.call(self, err, val, key);
    });

  return this;
};

// A low-level helper method. See increment().
KyotoDB.prototype._inc = function(method, key, num, orig, next) {
  var self = this;

  if (typeof orig != 'number') {
    next = orig;
    orig = 0;
  }

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](key, num, orig, function(err, value, key) {
      next.call(self, err, value, key);
    });

  return this;
};

// See getBulk(), setBulk(), &c
KyotoDB.prototype._bulk = function(method, what, atomic, next) {
  var self = this;

  if (typeof atomic == 'function') {
    next = atomic;
    atomic = undefined;
  }

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](what, !!atomic, function(err, result) {
      if (err)
        next.call(self, err);
      else
        next.call(self, null, result, what);
    });

  return this;
};

// See matchPrefix() &c
KyotoDB.prototype._match = function(method, pattern, max, next) {
  var self = this;

  if (typeof max == 'function') {
    next = max;
    max = -1;
  }

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](pattern, max, next);

  return this;
};

// See dumpSnapshot &c
KyotoDB.prototype._snap = function(method, path, next) {
  var self = this;

  next = next || noop;

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](path, function(err) {
      next.call(self, err);
    });

  return this;
};

// See size() &c
KyotoDB.prototype._stat = function(method, next) {
  var self = this;

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](function(err, info) {
      next.call(self, err, info);
    });

  return this;
};

// Create a cursor to iterate over items in the database.
//
// Returns Cursor instance.
KyotoDB.prototype.cursor = function() {
  return new Cursor(this);
};

// Iterate over all items in the database in an async-each style.
//
// The `fn` iterator is called with each item in the database in the
// context of a Cursor. It should handle the key/value pair and then
// call `next`. To stop iterating, it could call `this.stop()`.
//
// When an error is encountered or all items have been visited,
// `done` is called.
//
// + done - Function(Error) finished callback
// + fn   - Function(String value, String key, Function next) iterator
//
// Returns self.
KyotoDB.prototype.each = function(done, fn) {
  var self = this,
      wantsNext = false,
      finished = false,
      cursor = this.cursor();

  if (!fn) {
    fn = done;
    done = noop;
  }

  wantsNext = fn.length > 2;
  cursor.jump(function(err) {
    if (err && err.code === NOREC)
      finish();
    else if (err)
      finish(err);
    else
      step();
  });

  function step(err) {
    err ? finish(err) : cursor.get(true, dispatch);
  }

  function dispatch(err, val, key) {
    if (err)
      finish(err);
    else if (val === undefined)
      finish(err);
    else
      try {
        fn.call(cursor, val, key, step);
        wantsNext || step();
      } catch (x) {
        finish(x);
      }
  }

  function finish(err) {
    if (!finished) {
      finished = true;
      process.nextTick(function() { done.call(self, err); });
    }
  }

  return this;
};


// ## Cursor ##

// Construct a Cursor over a KyotoDB
//
// + db - KyotoDB instance
//
// Return self
function Cursor(db) {
  this.db = db;
  this.cursor = new K.Cursor(db.db);
}

// Get the current item.
//
// If there is no current item, call `next` with a `null` value.
//
// + step - Boolean step to next record afterward (optional, default: false)
// + next - Function(Error, String value, String key) callback
//
// Returns self
Cursor.prototype.get = function(step, next) {
  if (typeof step == 'function') {
    next = step;
    step = false;
  }

  this.cursor.get(step, function(err, val, key) {
    if (err && err.code == NOREC)
      next(null);
    else if (err)
      next(err);
    else
      next(null, val, key);
  });

  return this;
};

// Get the key of the current item.
//
// If there is no current item, call `next` with a `null` key.
//
// + step - Boolean step to next record afterward (optional, default: false)
// + next - Function(Error, String key) callback
//
// Returns self
Cursor.prototype.getKey = function(step, next) {
  if (typeof step == 'function') {
    next = step;
    step = false;
  }

  this.cursor.getKey(step, function(err, key) {
    if (err && err.code == NOREC)
      next(null);
    else if (err)
      next(err);
    else
      next(null, key);
  });

  return this;
};

// Get the value of the current item.
//
// If there is no current item, call `next` with a `null` value.
//
// + step - Boolean step to next record afterward (optional, default: false)
// + next - Function(Error, String value, String value) callback
//
// Returns self
Cursor.prototype.getValue = function(step, next) {
  if (typeof step == 'function') {
    next = step;
    step = undefined;
  }

  this.cursor.getValue(!!step, function(err, val) {
    if (err && err.code == NOREC)
      next(null);
    else if (err)
      next(err);
    else
      next(null, val);
  });

  return this;
};

// Set the value of the current item.
//
// + step - Boolean step to next record afterward (optional, default: false)
// + next - Function(Error) callback
//
// Returns self
Cursor.prototype.setValue = function(value, step, next) {
  if (typeof step == 'function') {
    next = step;
    step = undefined;
  }

  this.cursor.setValue(value, !!step, next);

  return this;
};

// Remove the current item.
//
// If there is no current item, call `next` with a `null` value.
//
// + step - Boolean step to next record afterward (optional, default: false)
// + next - Function(Error) callback
//
// Returns self
Cursor.prototype.remove = function(next) {
  this.cursor.remove(next);
  return this;
};

// Scan forward to a particular key (or prefix).
//
// If `to` is not given, move to the first record.
//
// + to   - String jump to this key (optional)
// + next - Function(Error) callback
//
// Returns self
Cursor.prototype.jump = function(to, next) {
  if (typeof to == 'function') {
    next = to;
    to = undefined;
  }

  to ? this.cursor.jumpTo(to, next) : this.cursor.jump(next);

  return this;
};

// Scan backward to a particular key (or prefix).
//
// If `to` is not given, move to the last record.
//
// + to   - String jump to this key (optional)
// + next - Function(Error) callback
//
// Returns self
Cursor.prototype.jumpBack = function(to, next) {
  if (typeof to == 'function') {
    next = to;
    to = undefined;
  }

  to ? this.cursor.jumpBackTo(to, next) : this.cursor.jumpBack(next);

  return this;
};

// Move forward to the next record.
//
// + next - Function(Error) callback
//
// Returns self
Cursor.prototype.step = function(next) {
  this.cursor.step(next);
  return this;
};

// Move backward to the previous record.
//
// + next - Function(Error) callback
//
// Returns self
Cursor.prototype.stepBack = function(next) {
  this.cursor.stepBack(next);
  return this;
};


// ## Helpers ##

function noop(err) {
  if (err) throw err;
}

function parseMode(mode) {

  if (typeof mode == 'number')
    return mode;

  switch(mode) {
  case 'r':
    return K.PolyDB.OREADER;
  case 'r+':
    return K.PolyDB.OWRITER;
  case 'w+':
    return K.PolyDB.OWRITER | K.PolyDB.OCREATE | K.PolyDB.OTRUNCATE;
  case 'a+':
    return K.PolyDB.OWRITER | K.PolyDB.OCREATE;
  default:
    return null;
  }
}


// ## Extensions for Toji ##

// These extensions are here for Toji and will remain undocumented for
// a while. They may change dramatically without notice.

KyotoDB.prototype.addIndexed = function(key, val, newIdx, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error('addIndexed: database is closed.'));
  else
    this.db.addIndexed(key, val, newIdx, function(err) {
      next.call(self, err, val, key);
    });

  return this;
};

KyotoDB.prototype.replaceIndexed = function(key, val, newIdx, removeKeys, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error('replaceIndexed: database is closed.'));
  else
    this.db.replaceIndexed(key, val, newIdx, removeKeys, function(err) {
      next.call(self, err, val, key);
    });

  return this;
};

KyotoDB.prototype.removeIndexed = function(key, removeKeys, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error('removeIndexed: database is closed.'));
  else
    this.db.removeIndexed(key, removeKeys, function(err) {
      next.call(self, err);
    });

  return this;
};

KyotoDB.prototype.modifyIndexed = function(method, key, val, newIdx, removeKeys, next) {
  var self = this;

  if (!next)
    next = noop;

  if (this.db === null)
    next.call(this, new Error(method + ': database is closed.'));
  else
    this.db[method](key, val, newIdx, removeKeys, function(err) {
      next.call(self, err, val, key);
    });

  return this;
};

KyotoDB.prototype.generate = function(jumpTo, done) {
  return new Generator(this, jumpTo, done);
};

Cursor.prototype.getKeyBlock = function(size, next) {
  this.cursor.getKeyBlock(size, function(err, block) {
    if (err && err.code == NOREC)
      next(null);
    else if (err)
      next(err);
    else
      next(null, block);
  });
};


// ## Generator ##

function Generator(db, jumpTo, done) {
  this.cursor = new K.Cursor(db.db);
  this.started = false;
  this.jumpTo = jumpTo;
  this.done = done;
}

Generator.prototype.then = function(callback) {
  this.done = callback(this.done);
  return this;
};

Generator.prototype.next = function(fn) {
  var self = this,
      cursor = this.cursor;

  if (this.started)
    step();
  else {
    this.started = true;
    jump();
  }

  function jump() {
    if (self.jumpTo)
      cursor.jumpTo(self.jumpTo, jumped);
    else
      cursor.jump(jumped);
  }

  function jumped(err) {
    if (err && err.code == NOREC)
      self.done();
    else if (err)
      self.done(err);
    else
      step();
  }

  function step() {
    cursor.get(true, emit);
  }

  function emit(err, val, key) {
    if (!key || (err && err.code == NOREC))
      self.done();
    else if (err)
      self.done(err);
    else
      fn.call(self, val, key);
  }

  return this;
};
