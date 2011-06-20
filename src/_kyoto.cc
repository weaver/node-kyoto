// # _kyoto.cc #
//
// Bindings to the kyotocabinet library. Here's a rough table of
// contents for this file:
//
// + Macros     - utilities, DEFINE_* methods for libeio
// + Maps/Lists - convert between stdlib and V8
// + PolyDB     - ObjectWrap around a PolyDB
// + Cursor     - ObjectWrap around a Cursor
// + Init       - module initialization

#include <v8.h>
#include <node.h>
#include <kcpolydb.h>

using namespace std;
using namespace node;
using namespace v8;
using namespace kyotocabinet;

// FIXME: is there a way to add a version check against kyotocabinet?


// ## Helpful Macros ##

#define SET_CONSTANT(target, name)					\
  (target)->Set(String::NewSymbol(#name),				\
                Integer::New(name),					\
                static_cast<PropertyAttribute>(ReadOnly|DontDelete))	\

#define SET_CLASS_CONSTANT(target, cls, name)				\
  (target)->Set(String::NewSymbol(#name),				\
                Integer::New(cls::name),				\
                static_cast<PropertyAttribute>(ReadOnly|DontDelete))	\

#define THROW_BAD_ARGS							\
  ThrowException(Exception::TypeError(String::New("Bad argument")))	\

#define LNULL								\
  Local<Value>::New(Null())						\

#define WRAP_STRING(str)                                                \
  String::New(str.c_str(), str.length());                               \

#define V8_TO_BOOL(obj)                                                 \
  (obj->ToBoolean() == v8::True())                                      \

#define BOOL_TO_LOCAL_V8(obj)                                           \
  (Local<Boolean>::New(Boolean::New(obj)))				\

#define EQ_STRING_BUF(str, buf, bsiz)                                   \
  (str.length() == bsiz && str.compare(0, bsiz, vbuf, bsiz) == 0)       \

#define EQ_UTF8_BUF(utf, buf, bsiz)                                     \
  ((size_t)utf.length() == bsiz && strncmp(*utf, buf, bsiz) == 0)       \

#define DEFINE_FUNC(Name, Request)					\
  static Handle<Value> Name(const Arguments& args) {			\
    HandleScope scope;							\
									\
    if (!Request::validate(args)) {					\
      return THROW_BAD_ARGS;						\
    }									\
									\
    Request* req = new Request(args);					\
									\
    eio_custom(EIO_Exec##Name, EIO_PRI_DEFAULT, EIO_After##Name, req);	\
    ev_ref(EV_DEFAULT_UC);						\
									\
    return args.This();							\
  }									\

#define DEFINE_EXEC(Name, Request)					\
  static int EIO_Exec##Name(eio_req *ereq) {				\
    Request* req = static_cast<Request *>(ereq->data);			\
    return req->exec();							\
  }									\

#define DEFINE_AFTER(Name, Request)					\
  static int EIO_After##Name(eio_req *ereq) {				\
    HandleScope scope;							\
    Request* req = static_cast<Request *>(ereq->data);			\
    ev_unref(EV_DEFAULT_UC);						\
    int result = req->after();						\
    delete req;								\
    return result;							\
  }									\

#define DEFINE_METHOD(Name, Request)					\
  DEFINE_FUNC(Name, Request)						\
  DEFINE_EXEC(Name, Request)						\
  DEFINE_AFTER(Name, Request)


// ## Maps and Lists ##

typedef std::vector<std::string> StringList;
typedef StringList::const_iterator StringIterator;
typedef std::map<std::string, std::string> StringMap;
typedef pair<std::string, std::string> MapItem;
typedef StringMap::const_iterator MapIterator;

void ObjToMap(const Local<Value> value, StringMap &result) {
  HandleScope scope;

  Local<Object> obj = Local<Object>::Cast(value);
  Local<Array> names = obj->GetPropertyNames();
  int names_len = names->Length();

  for (int i = 0; i < names_len; i++) {
    Local<Value> name = names->Get(Integer::New(i));
    String::Utf8Value key(name);
    String::Utf8Value val(obj->Get(name)->ToString());
    std::string std_key = std::string(*key, key.length());
    std::string std_val = std::string(*val, val.length());
    result.insert(MapItem(std_key, std_val));
  }
}

Local<Object> MapToObj(StringMap &map) {
  HandleScope scope;

  MapIterator item = map.begin();
  MapIterator end = map.end();


  Local<Object> result = Object::New();
  while (item != end) {
    Local<String> key = String::New(item->first.c_str(), item->first.length());
    Local<String> val = String::New(item->second.c_str(), item->second.length());
    result->Set(key, val);
    ++item;
  }

  return scope.Close(result);
}

void MapKeys(const StringMap &map, StringList &keys) {
  keys.reserve(map.size());
  MapIterator item = map.begin();
  MapIterator end = map.end();
  while(item != end) {
    keys.push_back(item->first);
    ++item;
  }
}

void ArrayToList(const Local<Value> obj, StringList &result) {
  HandleScope scope;

  Local<Array> array = Local<Array>::Cast(obj);
  int alen = array->Length();
  for (int i = 0; i < alen; i++) {
    String::Utf8Value val(array->Get(Integer::New(i))->ToString());
    result.push_back(std::string(*val, val.length()));
  }
}

Local<Object> ListToArray(StringList &list) {
  HandleScope scope;

  StringIterator item = list.begin();
  StringIterator end = list.end();

  Local<Array> result = Array::New(list.size());
  uint32_t index = 0;
  while(item != end) {
    Local<String> val = String::New(item->c_str(), item->length());
    result->Set(index++, val);
    ++item;
  }

  return scope.Close(result);
}


// ## PolyDB ##

class PolyDBWrap: ObjectWrap {
private:
  PolyDB* db;

public:

  
  // ### Initialization ###

  static Persistent<FunctionTemplate> ctor;

  static void Init(Handle<Object> target) {
    HandleScope scope;

    Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);

    ctor = Persistent<FunctionTemplate>::New(tmpl);
    ctor->InstanceTemplate()->SetInternalFieldCount(1);
    ctor->SetClassName(String::NewSymbol("PolyDB"));

    SET_CLASS_CONSTANT(ctor, PolyDB, OREADER);
    SET_CLASS_CONSTANT(ctor, PolyDB, OWRITER);
    SET_CLASS_CONSTANT(ctor, PolyDB, OCREATE);
    SET_CLASS_CONSTANT(ctor, PolyDB, OTRUNCATE);
    SET_CLASS_CONSTANT(ctor, PolyDB, OAUTOTRAN);
    SET_CLASS_CONSTANT(ctor, PolyDB, OAUTOSYNC);
    SET_CLASS_CONSTANT(ctor, PolyDB, ONOLOCK);
    SET_CLASS_CONSTANT(ctor, PolyDB, OTRYLOCK);
    SET_CLASS_CONSTANT(ctor, PolyDB, ONOREPAIR);

    SET_CLASS_CONSTANT(ctor, PolyDB::Error, SUCCESS);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, NOIMPL);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, INVALID);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, NOREPOS);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, NOPERM);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, BROKEN);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, DUPREC);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, NOREC);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, LOGIC);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, SYSTEM);
    SET_CLASS_CONSTANT(ctor, PolyDB::Error, MISC);

    SET_CONSTANT(ctor, INT64MIN);
    SET_CONSTANT(ctor, INT64MAX);

    NODE_SET_PROTOTYPE_METHOD(ctor, "open", Open);
    NODE_SET_PROTOTYPE_METHOD(ctor, "close", Close);
    NODE_SET_PROTOTYPE_METHOD(ctor, "closeSync", CloseSync);
    NODE_SET_PROTOTYPE_METHOD(ctor, "clear", Clear);
    NODE_SET_PROTOTYPE_METHOD(ctor, "set", Set);
    NODE_SET_PROTOTYPE_METHOD(ctor, "add", Add);
    NODE_SET_PROTOTYPE_METHOD(ctor, "replace", Replace);
    NODE_SET_PROTOTYPE_METHOD(ctor, "append", Append);
    NODE_SET_PROTOTYPE_METHOD(ctor, "increment", Increment);
    NODE_SET_PROTOTYPE_METHOD(ctor, "incrementDouble", IncrementDouble);
    NODE_SET_PROTOTYPE_METHOD(ctor, "cas", CAS);
    NODE_SET_PROTOTYPE_METHOD(ctor, "remove", Remove);
    NODE_SET_PROTOTYPE_METHOD(ctor, "get", Get);
    NODE_SET_PROTOTYPE_METHOD(ctor, "getBulk", GetBulk);
    NODE_SET_PROTOTYPE_METHOD(ctor, "setBulk", SetBulk);
    NODE_SET_PROTOTYPE_METHOD(ctor, "removeBulk", RemoveBulk);
    NODE_SET_PROTOTYPE_METHOD(ctor, "matchPrefix", MatchPrefix);
    NODE_SET_PROTOTYPE_METHOD(ctor, "matchRegex", MatchRegex);
    NODE_SET_PROTOTYPE_METHOD(ctor, "synchronize", Synchronize);

    // Here are some non-standard methods for Toji.
    NODE_SET_PROTOTYPE_METHOD(ctor, "addIndexed", AddIndexed);
    NODE_SET_PROTOTYPE_METHOD(ctor, "replaceIndexed", ReplaceIndexed);
    NODE_SET_PROTOTYPE_METHOD(ctor, "removeIndexed", RemoveIndexed);

    target->Set(String::NewSymbol("PolyDB"), ctor->GetFunction());
  }

  
  // ### Construction ###

  PolyDBWrap()  {
    db = new PolyDB();
  }

  ~PolyDBWrap() {
    delete db;
  }

  static Handle<Value> New(const Arguments& args) {
    HandleScope scope;
    PolyDBWrap* wrap = new PolyDBWrap();
    wrap->Wrap(args.This());
    return args.This();
  }

  
  // ### Helpers ###

  DB::Cursor* cursor() {
    return db->cursor();
  }

  class Request {
  private:
    Persistent<String> code_symbol;

  protected:
    PolyDBWrap* wrap;
    Persistent<Function> next;
    PolyDB::Error::Code result;

  public:
    Request(const Arguments& args, int nextIndex):
      result(PolyDB::Error::SUCCESS) {
      HandleScope scope;

      wrap = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
      next = Persistent<Function>::New(Handle<Function>::Cast(args[nextIndex]));

      wrap->Ref();
    }

    ~Request() {
      wrap->Unref();
      next.Dispose();
    }

    virtual inline int exec() = 0;

    virtual inline int after() = 0;

    inline void callback(int argc, Handle<Value> argv[]) {
      TryCatch try_catch;
      next->Call(Context::GetCurrent()->Global(), argc, argv);
      if (try_catch.HasCaught()) {
	FatalException(try_catch);
      }
    }

    Local<Value> error() {
      if (result == PolyDB::Error::SUCCESS)
	return LNULL;

      const char* name = PolyDB::Error::codename(result);
      Local<String> message = String::NewSymbol(name);
      Local<Value> err = Exception::Error(message);

      if (code_symbol.IsEmpty()) {
	code_symbol = NODE_PSYMBOL("code");
      }

      Local<Object> obj = err->ToObject();
      obj->Set(code_symbol, Integer::New(result));

      return err;
    }
  };

  
  // ### Open ###

  DEFINE_METHOD(Open, OpenRequest)
  class OpenRequest: public Request {
  private:
    String::Utf8Value path;
    uint32_t mode;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && args[1]->IsUint32()
	      && args[2]->IsFunction());
    }

    OpenRequest(const Arguments& args):
      Request(args, 2),
      path(args[0]->ToString()),
      mode(args[1]->Uint32Value())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->open(*path, mode)) result = db->error().code();
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  
  // ### Close ###

  DEFINE_METHOD(Close, CloseRequest)
  class CloseRequest: public Request {
  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 1 && args[0]->IsFunction());
    }

    CloseRequest(const Arguments& args):
      Request(args, 0)
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->close()) result = db->error().code();
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  static Handle<Value> CloseSync(const Arguments& args) {
    HandleScope scope;

    PolyDBWrap* wrap = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
    PolyDB* db = wrap->db;

    return Boolean::New(db->close());
  }

  
  // ### Clear ###

  DEFINE_METHOD(Clear, ClearRequest)
  class ClearRequest: public CloseRequest {
  public:

    ClearRequest(const Arguments& args):
      CloseRequest(args)
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->clear()) result = db->error().code();
      return 0;
    }
  };

  
  // ### Set ###

  DEFINE_METHOD(Set, SetRequest)
  class SetRequest: public Request {
  protected:
    String::Utf8Value key;
    String::Utf8Value value;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && args[1]->IsString()
	      && args[2]->IsFunction());
    }

    SetRequest(const Arguments& args):
      Request(args, 2),
      key(args[0]->ToString()),
      value(args[1]->ToString())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->set(*key, key.length(), *value, value.length())) {
	result = db->error().code();
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  
  // ### Add ###

  DEFINE_METHOD(Add, AddRequest)
  class AddRequest: public SetRequest {
  public:
    AddRequest(const Arguments& args) :
      SetRequest(args)
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->add(*key, key.length(), *value, value.length())) {
	result = db->error().code();
      }
      return 0;
    }
  };

  
  // ### Replace ###

  DEFINE_METHOD(Replace, ReplaceRequest)
  class ReplaceRequest: public SetRequest {
  public:
    ReplaceRequest(const Arguments& args) :
      SetRequest(args)
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->replace(*key, key.length(), *value, value.length())) {
	result = db->error().code();
      }
      return 0;
    }
  };

  
  // ### Append ###

  DEFINE_METHOD(Append, AppendRequest)
  class AppendRequest: public SetRequest {
  public:
    AppendRequest(const Arguments& args) :
      SetRequest(args)
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->append(*key, key.length(), *value, value.length())) {
	result = db->error().code();
      }
      return 0;
    }
  };

  
  // ### Increment ###

  DEFINE_METHOD(Increment, IncrementRequest)
  class IncrementRequest: public Request {
  protected:
    String::Utf8Value key;
    int64_t num;
    int64_t orig;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && args[1]->IsNumber()
	      && args[2]->IsNumber()
	      && args[3]->IsFunction());
    }

    IncrementRequest(const Arguments& args):
      Request(args, 3),
      key(args[0]->ToString()),
      num(args[1]->IntegerValue()),
      orig(args[2]->IntegerValue())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;

      num = db->increment(*key, key.length(), num, orig);
      if (num == INT64MIN) {
	result = db->error().code();
      }

      return 0;
    }

    inline int after() {
      int argc = 0;
      Local<Value> argv[2];

      argv[argc++] = error();
      if (num != INT64MIN) {
	argv[argc++] = Integer::New(num);
      }

      callback(argc, argv);
      return 0;
    }
  };

  
  // ### IncrementDouble ###

  DEFINE_METHOD(IncrementDouble, IncrementDoubleRequest)
  class IncrementDoubleRequest: public Request {
  protected:
    String::Utf8Value key;
    double num;
    double orig;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && args[1]->IsNumber()
	      && args[2]->IsNumber()
	      && args[3]->IsFunction());
    }

    IncrementDoubleRequest(const Arguments& args):
      Request(args, 3),
      key(args[0]->ToString()),
      num(args[1]->NumberValue()),
      orig(args[2]->NumberValue())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;

      num = db->increment_double(*key, key.length(), num, orig);
      if (std::isnan(num)) {
	result = db->error().code();
      }

      return 0;
    }

    inline int after() {
      int argc = 0;
      Local<Value> argv[2];

      argv[argc++] = error();
      if (!std::isnan(num)) {
	argv[argc++] = Number::New(num);
      }

      callback(argc, argv);
      return 0;
    }
  };

  
  // ### CAS ###

  DEFINE_METHOD(CAS, CASRequest)
  class CASRequest: public Request {
  protected:
    String::Utf8Value key;
    String::Utf8Value *ovalue;
    String::Utf8Value *nvalue;
    bool success;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && (args[1]->IsString() || args[1]->IsNull())
	      && (args[2]->IsString() || args[2]->IsNull())
	      && args[3]->IsFunction());
    }

    CASRequest(const Arguments& args):
      Request(args, 3),
      key(args[0]->ToString()),
      ovalue(NULL),
      nvalue(NULL)
    {
      if (args[1]->IsString()) {
	ovalue = new String::Utf8Value(args[1]->ToString());
      }

      if (args[2]->IsString()) {
	nvalue = new String::Utf8Value(args[2]->ToString());
      }
    }

    ~CASRequest() {
      if (ovalue) delete ovalue;
      if (nvalue) delete nvalue;
    }

    inline int exec() {
      PolyDB* db = wrap->db;

      success = db->cas(*key, key.length(),
			(ovalue == NULL ? NULL : **ovalue),
			(ovalue == NULL ? 0 : ovalue->length()),
			(nvalue == NULL ? NULL : **nvalue),
			(nvalue == NULL ? 0 : nvalue->length()));

      if (!success) {
	result = db->error().code();
      }

      return 0;
    }

    inline int after() {
      Local<Value> argv[2] = {
	(result == PolyDB::Error::LOGIC) ? LNULL : error(),
	BOOL_TO_LOCAL_V8(success)
      };
      callback(2, argv);
      return 0;
    }
  };

  
  // ### Get ###

  DEFINE_METHOD(Get, GetRequest)
  class GetRequest: public Request {
  protected:
    String::Utf8Value key;
    char *vbuf;
    size_t vsiz;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 2
	      && args[0]->IsString()
	      && args[1]->IsFunction());
    }

    GetRequest(const Arguments& args):
      Request(args, 1),
      key(args[0]->ToString())
    {}

    ~GetRequest() {
      if (vbuf) delete[] vbuf;
    }

    inline int exec() {
      PolyDB* db = wrap->db;
      vbuf = db->get(*key, key.length(), &vsiz);
      if (!vbuf) result = db->error().code();
      return 0;
    }

    inline int after() {
      int argc = 1;
      Local<Value> argv[2];

      argv[0] = error();
      if (vbuf) argv[argc++] = String::New(vbuf, vsiz);

      callback(argc, argv);
      return 0;
    }
  };

  
  // ### GetBulk ###

  DEFINE_METHOD(GetBulk, GetBulkRequest)
  class GetBulkRequest: public Request {
  protected:
    StringList keys;
    StringMap items;
    bool atomic;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsArray()
	      && args[1]->IsBoolean()
	      && args[2]->IsFunction());
    }

    GetBulkRequest(const Arguments& args):
      Request(args, 2),
      atomic(V8_TO_BOOL(args[1]))
    {
      ArrayToList(args[0], keys);
    }

    inline int exec() {
      PolyDB* db = wrap->db;
      if (db->get_bulk(keys, &items, atomic) == -1) {
	result = db->error().code();
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[2] = { error(), MapToObj(items) };
      callback(2, argv);
      return 0;
    }
  };

  
  // ### SetBulk ###

  DEFINE_METHOD(SetBulk, SetBulkRequest)
  class SetBulkRequest: public Request {
  protected:
    StringMap items;
    bool atomic;
    int64_t stored;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsObject()
	      && args[1]->IsBoolean()
	      && args[2]->IsFunction());
    }

    SetBulkRequest(const Arguments& args):
      Request(args, 2),
      atomic(V8_TO_BOOL(args[1]))
    {
      ObjToMap(args[0], items);
    }

    inline int exec() {
      PolyDB* db = wrap->db;

      stored = db->set_bulk(items, atomic);
      if (stored == -1) {
	result = db->error().code();
      }

      return 0;
    }

    inline int after() {
      Local<Value> argv[2] = { error(), Integer::New(stored) };
      callback(2, argv);
      return 0;
    }
  };

  
  // ### RemoveBulk ###

  DEFINE_METHOD(RemoveBulk, RemoveBulkRequest)
  class RemoveBulkRequest: public Request {
  protected:
    StringList keys;
    bool atomic;
    int64_t removed;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsArray()
	      && args[1]->IsBoolean()
	      && args[2]->IsFunction());
    }

    RemoveBulkRequest(const Arguments& args):
      Request(args, 2),
      atomic(V8_TO_BOOL(args[1]))
    {
      ArrayToList(args[0], keys);
    }

    inline int exec() {
      PolyDB* db = wrap->db;
      removed = db->remove_bulk(keys, atomic);
      if (removed == -1) {
	result = db->error().code();
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[2] = { error(), Integer::New(removed) };
      callback(2, argv);
      return 0;
    }
  };

  
  // ### Remove ###

  DEFINE_METHOD(Remove, RemoveRequest)
  class RemoveRequest: public Request {
  protected:
    String::Utf8Value key;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 1
	      && args[0]->IsString());
    }

    RemoveRequest(const Arguments& args):
      Request(args, 1),
      key(args[0]->ToString())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->remove(*key, key.length())) {
	result = db->error().code();
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  
  // ### MatchPrefix ###

  DEFINE_METHOD(MatchPrefix, MatchPrefixRequest)
  class MatchPrefixRequest: public Request {
  protected:
    String::Utf8Value pattern;
    int64_t max;
    StringList keys;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && args[1]->IsNumber()
	      && args[2]->IsFunction());
    }

    MatchPrefixRequest(const Arguments& args):
      Request(args, 2),
      pattern(args[0]->ToString()),
      max(args[1]->IntegerValue())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;

      std::string prefix = std::string(*pattern, pattern.length());
      if (db->match_prefix(prefix, &keys, max) == -1) {
	result = db->error().code();
      }

      return 0;
    }

    inline int after() {
      Local<Value> argv[2] = { error(), ListToArray(keys) };
      callback(2, argv);
      return 0;
    }
  };

  
  // ### MatchRegex ###

  DEFINE_METHOD(MatchRegex, MatchRegexRequest)
  class MatchRegexRequest: public Request {
  protected:
    String::Utf8Value pattern;
    int64_t max;
    StringList keys;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && args[1]->IsNumber()
	      && args[2]->IsFunction());
    }

    MatchRegexRequest(const Arguments& args):
      Request(args, 2),
      pattern(args[0]->ToString()),
      max(args[1]->IntegerValue())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;

      std::string regex = std::string(*pattern, pattern.length());
      if (db->match_regex(regex, &keys, max) == -1) {
	result = db->error().code();
      }

      return 0;
    }

    inline int after() {
      Local<Value> argv[2] = { error(), ListToArray(keys) };
      callback(2, argv);
      return 0;
    }
  };

  
  // ### Synchronize ###

  DEFINE_METHOD(Synchronize, SynchronizeRequest)
  class SynchronizeRequest: public Request {
  protected:
    bool hard;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 1
	      && args[0]->IsBoolean());
    }

    SynchronizeRequest(const Arguments& args):
      Request(args, 1),
      hard(args[0]->ToBoolean() == v8::True())
    {}

    inline int exec() {
      PolyDB* db = wrap->db;
      if (!db->synchronize(hard)) {
	result = db->error().code();
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  
  // ## Toji Support ##

  // These methods are here to support Toji. They're not part of the
  // public API and may change dramatically between releases.

  class ApplyIndexVisitor : public DB::Visitor {
  public:
    String::Utf8Value& key;
    const StringMap& index;
    StringMap& errors;

    explicit ApplyIndexVisitor(String::Utf8Value &key, const StringMap& index, StringMap& errors) :
      key(key),
      index(index),
      errors(errors)
    {}

  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
			   const char* vbuf, size_t vsiz,
			   size_t *sp)
    {
      MapIterator probe = index.find(std::string(kbuf, ksiz));

      // It's an error for an index entry to exist with any other
      // value than the value it's supposed to be.
      if (probe != index.end() && !EQ_STRING_BUF(probe->second, vbuf, vsiz)) {
	errors.insert(MapItem(probe->first, std::string(vbuf, vsiz)));
      }

      return NOP;
    }

    const char* visit_empty(const char* kbuf, size_t ksiz,
			    size_t *sp)
    {
      MapIterator probe = index.find(std::string(kbuf, ksiz));
      if (probe == index.end()) {
	return NOP;
      }

      *sp = probe->second.size();
      return probe->second.data();
    }
  };

  class RemoveIndexVisitor : public DB::Visitor {
  public:
    String::Utf8Value& key;
    StringMap& errors;

    explicit RemoveIndexVisitor(String::Utf8Value &key, StringMap& errors) :
      key(key),
      errors(errors)
    {}

  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
			   const char* vbuf, size_t vsiz,
			   size_t *sp)
    {
      // It's an error to remove an index entry when it doesn't
      // point to this object.
      if (!EQ_UTF8_BUF(key, vbuf, vsiz)) {
	errors.insert(MapItem(std::string(kbuf, ksiz), std::string(vbuf, vsiz)));
	return NOP;
      }
      return REMOVE;
    }
  };

  class IndexedRequest: public Request {
  private:
    Persistent<String> invalid_symbol;

  protected:
    String::Utf8Value key;

    StringMap toIndex;
    StringList toRemove;
    StringMap errors;

  public:

    IndexedRequest(const Arguments &args, int nextIndex) :
      Request(args, nextIndex),
      key(args[0]->ToString())
    {}

    virtual bool main_operation() = 0;

    inline bool apply_index() {
      PolyDB* db = wrap->db;

      std::vector<std::string> keys;
      MapKeys(toIndex, keys);

      ApplyIndexVisitor visitor(key, toIndex, errors);
      int written = db->accept_bulk(keys, &visitor, true);

      return (written != -1) && errors.empty();
    }

    bool cleanup() {
      PolyDB* db = wrap->db;

      RemoveIndexVisitor visitor(key, errors);
      int written = db->accept_bulk(toRemove, &visitor, true);

      return (written != -1) && errors.empty();
    }

    inline int exec() {
      // Fast path: nothing to index, just run the main op.
      if (toIndex.empty() && toRemove.empty()) {
	if (!main_operation()) {
	  result = wrap->db->error().code();
	}
	return 0;
      }

      // Long path: run full transaction, update indicies.
      return transaction();
    }

    inline int transaction() {
      PolyDB* db = wrap->db;

      if (!db->begin_transaction()) {
	result = db->error().code();
	return 0;
      }

      if (!main_operation()) {
	result = db->error().code();
	db->end_transaction(false);
	return 0;
      }

      if (!toIndex.empty()) {
	if (!apply_index()) {
	  result = db->error().code();
	  db->end_transaction(false);
	  return 0;
	}
      }

      if (!toRemove.empty()) {
	if (!cleanup()) {
	  result = db->error().code();
	  db->end_transaction(false);
	  return 0;
	}
      }

      if (!db->end_transaction(true)) {
	result = db->error().code();
      }

      return 0;
    }

    Local<Value> error() {
      Local<Value> err = Request::error();

      if (!errors.empty()) {
	if (err->IsNull()) {
	  err = Exception::Error(String::NewSymbol("index-error"));
	}

	if (invalid_symbol.IsEmpty()) {
	  invalid_symbol = NODE_PSYMBOL("invalid");
	}

	Local<Object> obj = err->ToObject();
	obj->Set(invalid_symbol, MapToObj(errors));
      }

      return err;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  
  // ### AddIndexed ###

  DEFINE_METHOD(AddIndexed, AddIndexedRequest)
  class AddIndexedRequest: virtual public IndexedRequest {
  protected:
    String::Utf8Value value;

  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 4
	      && args[0]->IsString()
	      && args[1]->IsString()
	      && (args[2]->IsObject() || args[2]->IsNull())
	      && args[3]->IsFunction());
    }

    AddIndexedRequest(const Arguments& args):
      IndexedRequest(args, 3),
      value(args[1]->ToString())
    {
      if (!args[2]->IsNull()) {
	ObjToMap(args[2], toIndex);
      }
    }

    bool main_operation() {
      PolyDB* db = wrap->db;
      return db->add(*key, key.length(), *value, value.length());
    }
  };

  
  // ### ReplaceIndexed ###

  DEFINE_METHOD(ReplaceIndexed, ReplaceIndexedRequest)
  class ReplaceIndexedRequest: public IndexedRequest {
  protected:
    String::Utf8Value value;

  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 5
	      && args[0]->IsString()
	      && args[1]->IsString()
	      && (args[2]->IsObject() || args[2]->IsNull())
	      && (args[3]->IsArray() || args[3]->IsNull())
	      && args[4]->IsFunction());
    }

    ReplaceIndexedRequest(const Arguments& args):
      IndexedRequest(args, 4),
      value(args[1]->ToString())
    {
      if (!args[2]->IsNull()) {
	ObjToMap(args[2], toIndex);
      }
      if (!args[3]->IsNull()) {
	ArrayToList(args[3], toRemove);
      }
    }

    bool main_operation() {
      PolyDB* db = wrap->db;
      return db->replace(*key, key.length(), *value, value.length());
    }
  };

  
  // ### RemoveIndexed ###

  DEFINE_METHOD(RemoveIndexed, RemoveIndexedRequest)
  class RemoveIndexedRequest: public IndexedRequest {
  public:
    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 3
	      && args[0]->IsString()
	      && (args[1]->IsArray() || args[1]->IsNull())
	      && args[2]->IsFunction());
    }

    RemoveIndexedRequest(const Arguments& args):
      IndexedRequest(args, 2)
    {
      if (!args[1]->IsNull()) {
	ArrayToList(args[1], toRemove);
      }
    }

    bool main_operation() {
      PolyDB* db = wrap->db;
      return db->remove(*key, key.length());
    }
  };

};


// ## Cursor ##

#define CURSOR_ERROR(cursor)                                            \
  static_cast<PolyDB *>(cursor->db())->error().code()                   \

class CursorWrap: ObjectWrap {
private:
  DB::Cursor* cursor;

public:

  
  // ### Initialization ###

  static Persistent<FunctionTemplate> ctor;

  static void Init(Handle<Object> target) {
    HandleScope scope;

    Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);

    ctor = Persistent<FunctionTemplate>::New(tmpl);
    ctor->InstanceTemplate()->SetInternalFieldCount(1);
    ctor->SetClassName(String::NewSymbol("Cursor"));

    NODE_SET_PROTOTYPE_METHOD(ctor, "get", Get);
    NODE_SET_PROTOTYPE_METHOD(ctor, "getKey", GetKey);
    NODE_SET_PROTOTYPE_METHOD(ctor, "getKeyBlock", GetKeyBlock);
    NODE_SET_PROTOTYPE_METHOD(ctor, "getValue", GetValue);
    NODE_SET_PROTOTYPE_METHOD(ctor, "jump", Jump);
    NODE_SET_PROTOTYPE_METHOD(ctor, "jumpTo", JumpTo);
    NODE_SET_PROTOTYPE_METHOD(ctor, "jumpBack", JumpBack);
    NODE_SET_PROTOTYPE_METHOD(ctor, "jumpBackTo", JumpBackTo);
    NODE_SET_PROTOTYPE_METHOD(ctor, "step", Step);
    NODE_SET_PROTOTYPE_METHOD(ctor, "stepBack", StepBack);

    target->Set(String::NewSymbol("Cursor"), ctor->GetFunction());
  }

  
  // ### Construction ###

  CursorWrap(DB::Cursor* cur):
    cursor(cur)
  {}

  ~CursorWrap() {
    delete cursor;
  }

  static Handle<Value> New(const Arguments& args) {
    HandleScope scope;

    if (args.Length() < 1 && args[0]->IsObject()) return THROW_BAD_ARGS;

    PolyDBWrap* dbWrap = ObjectWrap::Unwrap<PolyDBWrap>(args[0]->ToObject());
    CursorWrap* cursorWrap = new CursorWrap(dbWrap->cursor());
    cursorWrap->Wrap(args.This());
    return args.This();
  }

  
  // ### Helpers ###

  class Request {
  private:
    Persistent<String> code_symbol;

  protected:
    CursorWrap* wrap;
    Persistent<Function> next;
    PolyDB::Error::Code result;

  public:
    Request(const Arguments& args, int nextIndex):
      result(PolyDB::Error::SUCCESS) {
      HandleScope scope;

      wrap = ObjectWrap::Unwrap<CursorWrap>(args.This());
      next = Persistent<Function>::New(Handle<Function>::Cast(args[nextIndex]));

      wrap->Ref();
    }

    ~Request() {
      wrap->Unref();
      next.Dispose();
    }

    inline void callback(int argc, Handle<Value> argv[]) {
      TryCatch try_catch;
      next->Call(Context::GetCurrent()->Global(), argc, argv);
      if (try_catch.HasCaught()) {
	FatalException(try_catch);
      }
    }

    Local<Value> error() {
      if (result == PolyDB::Error::SUCCESS)
	return LNULL;

      const char* name = PolyDB::Error::codename(result);
      Local<String> message = String::NewSymbol(name);
      Local<Value> err = Exception::Error(message);

      if (code_symbol.IsEmpty()) {
	code_symbol = NODE_PSYMBOL("code");
      }

      Local<Object> obj = err->ToObject();
      obj->Set(code_symbol, Integer::New(result));

      return err;
    }
  };

  
  // ### Get ###

  DEFINE_METHOD(Get, GetRequest)
  class GetRequest: public Request {
  private:
    bool step;
    std::string key, value;

  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 2
	      && args[0]->IsBoolean()
	      && args[1]->IsFunction());
    }

    GetRequest(const Arguments& args):
      Request(args, 1),
      step(V8_TO_BOOL(args[0]))
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->get(&key, &value, step)) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }

    inline int after() {
      int argc;
      Local<Value> argv[3];

      if (result == PolyDB::Error::SUCCESS) {
  	argc = 3;
  	argv[0] = LNULL;
  	argv[1] = WRAP_STRING(value);
  	argv[2] = WRAP_STRING(key);
      }
      else {
  	argc = 1;
  	argv[0] = (result == PolyDB::Error::NOREC) ? LNULL : error();
      }

      callback(argc, argv);
      return 0;
    }
  };

  
  // ### Get Key ###

  DEFINE_METHOD(GetKey, GetKeyRequest)
  class GetKeyRequest: public Request {
  protected:
    bool step;
    std::string value;

  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 2
	      && args[0]->IsBoolean()
	      && args[1]->IsFunction());
    }

    GetKeyRequest(const Arguments& args):
      Request(args, 1),
      step(V8_TO_BOOL(args[0]))
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->get_key(&value, step)) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }

    inline int after() {
      int argc;
      Local<Value> argv[2];

      if (result == PolyDB::Error::SUCCESS) {
  	argc = 2;
  	argv[0] = LNULL;
  	argv[1] = WRAP_STRING(value);
      }
      else {
  	argc = 1;
  	argv[0] = (result == PolyDB::Error::NOREC) ? LNULL : error();
      }

      callback(argc, argv);
      return 0;
    }
  };

  
  // ### Get Value ###

  DEFINE_METHOD(GetValue, GetValueRequest)
  class GetValueRequest: public GetKeyRequest {
  public:
    GetValueRequest(const Arguments& args):
      GetKeyRequest(args)
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->get_value(&value, step)) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }
  };

  
  // ### Get Key Block ###

  DEFINE_METHOD(GetKeyBlock, GetKeyBlockRequest)
  class GetKeyBlockRequest: public Request {
  protected:
    uint32_t size;
    StringList keys;

  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 2
	      && args[0]->IsUint32()
	      && args[1]->IsFunction());
    }

    GetKeyBlockRequest(const Arguments& args):
      Request(args, 1),
      size(args[0]->Uint32Value())
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;

      std::string key;
      for (uint32_t i = 0; i < size; i++) {
	if (!cursor->get_key(&key, true)) {
	  result = CURSOR_ERROR(cursor);
	  break;
	}
	keys.push_back(key);
      }

      return 0;
    }

    inline int after() {
      int argc;
      Local<Value> argv[2];

      if (result == PolyDB::Error::SUCCESS) {
  	argc = 2;
  	argv[0] = LNULL;
  	argv[1] = ListToArray(keys);
      }
      else {
  	argc = 1;
  	argv[0] = (result == PolyDB::Error::NOREC) ? LNULL : error();
      }

      callback(argc, argv);
      return 0;
    }
  };

  
  // ### Jump ###

  DEFINE_METHOD(Jump, JumpRequest)
  class JumpRequest: public Request {

  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 1 && args[0]->IsFunction());
    }

    JumpRequest(const Arguments& args):
      Request(args, 0)
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->jump()) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  DEFINE_METHOD(JumpTo, JumpToRequest)
  class JumpToRequest: public Request {
  protected:
    String::Utf8Value key;

  public:

    inline static bool validate(const Arguments& args) {
      return (args.Length() >= 2
	      && args[0]->IsString()
	      && args[1]->IsFunction());
    }

    JumpToRequest(const Arguments& args):
      Request(args, 1),
      key(args[0]->ToString())
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->jump(*key, key.length())) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }

    inline int after() {
      Local<Value> argv[1] = { error() };
      callback(1, argv);
      return 0;
    }
  };

  
  // ### Jump Back ###

  DEFINE_METHOD(JumpBack, JumpBackRequest)
  class JumpBackRequest: public JumpRequest {

  public:

    JumpBackRequest(const Arguments& args):
      JumpRequest(args)
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->jump_back()) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }
  };

  DEFINE_METHOD(JumpBackTo, JumpBackToRequest)
  class JumpBackToRequest: public JumpToRequest {

  public:

    JumpBackToRequest(const Arguments& args):
      JumpToRequest(args)
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->jump_back(*key, key.length())) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }
  };

  
  // ### Step ###

  DEFINE_METHOD(Step, StepRequest)
  class StepRequest: public JumpRequest {

  public:

    StepRequest(const Arguments& args):
      JumpRequest(args)
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->step()) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }
  };

  
  // ### Step Back ###

  DEFINE_METHOD(StepBack, StepBackRequest)
  class StepBackRequest: public JumpRequest {

  public:

    StepBackRequest(const Arguments& args):
      JumpRequest(args)
    {}

    inline int exec() {
      DB::Cursor* cursor = wrap->cursor;
      if (!cursor->step_back()) {
	result = CURSOR_ERROR(cursor);
      }
      return 0;
    }
  };

};


// ## Init ##

Persistent<FunctionTemplate> PolyDBWrap::ctor;
Persistent<FunctionTemplate> CursorWrap::ctor;

extern "C" {
  static void init (Handle<Object> target) {
    PolyDBWrap::Init(target);
    CursorWrap::Init(target);
  }

  NODE_MODULE(_kyoto, init);
}
