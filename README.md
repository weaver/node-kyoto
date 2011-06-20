# kyoto - Kyoto Cabinet bindings for Node.JS #

## Synopsis ##

Kyoto Cabinet is a library of routines for managing a database. It
supports fast, efficient hash-based and tree-based storage for
key/value data. See [fundamental specifications][1] or
[API reference][2] for more details.

This package implements bindings to the `kyotocabinet` library for
Node.JS. For example:

    var Kyoto = require('kyoto');
    var db = Kyoto.open('/tmp/data.kch', 'a+', onOpen);

    function onOpen(err) {
      if (err) throw err;
      db.set('my', 'value', query);
    }

    function query(err) {
      if (err) throw err;
      db.get('my', function(err, value) {
        if (err) throw err;
        console.log('My value is', value);
      });
    }

    process.on('exit', function() {
      db.closeSync();
    });

    process.on('uncaughtException', function(err) {
      // Do something with err
      db.closeSync();
    });

**Note: it's very important to close the database property. Be sure to
  install `exit` and `uncaughtException` handlers that call
  `.closeSync()`.**

[1]: http://fallabs.com/kyotocabinet/spex.html
[2]: http://fallabs.com/kyotocabinet/api/

## Documentation ##

Please refer to `kyoto.js` for documentation of each method. Some
`kyotocabinet` methods aren't implemented yet. More docs and examples
are forthcoming.

## Related ##

An alternative to this package is [Kyoto Client][3], an implementation
of a Kyoto Tycoon client. Tycoon is an out-of-process server that
allows clients to interact with a Kyoto Cabinet over a network
API. Using Kyoto Client is recommended unless you really need
something in-process and plan to implement your own key/value network
API.

## License ##

Copyright (c) 2011, Ben Weaver &lt;ben@orangesoda.net&gt;
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT
HOLDER> BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


