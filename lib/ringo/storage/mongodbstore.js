require('core/string');
var {addHostObject} = require('ringo/engine');
var {bindArguments} = require('ringo/functional');

var log = require('ringo/logging').getLogger(module.id);

export("Store");

importPackage(com.mongodb);
addHostObject(org.ringojs.wrappers.Storable);

function Store(host, port, dbName, options) {
    var self = this;
    var collections = {};
    var registry = {};

    var db = openMongoDb(host, port, dbName);

    this.defineEntity = function(type) {
        var ctor = registry[type];
        if (!ctor) {
            ctor = registry[type] = Storable.defineEntity(self, type);
            ctor.all = bindArguments(all, type);
            ctor.get = bindArguments(get, type);
            ctor.query = bindArguments(query, type);
        }
        return ctor;
    };

    function create(type, key, entity) {
        var ctor = registry[type];
        if (!ctor) {
            throw new Error('Entity "' + type + '" is not defined');
        }
        return ctor.createInstance(key, entity);
    }

    function all(type) {
        return getCollection(type).all();
    }

    function get(type, id) {
        return getCollection(type).get(id);
    }

    function query(type) {
        return new BaseQuery(type);
    }

    this["remove"] = function(key){
        return getCollection(extractTypeFromKey(key)).remove(key);
    };
    
    this.save = function(props, entity, entities) {
        if (entities && entities.contains(entity)) {
            return;
        }
        var isRoot = false;
        if (!entities) {
            isRoot = true;
            entities = new java.util.HashSet();
        }
        entities.add(entity);
        for (var id in props) {
            var value = props[id];
            if (isStorable(value)) {
                value.save(entities);
                value = value._id;
            } else if (value instanceof Array) {
                var list = new java.util.ArrayList();
                value.forEach(function(obj) {
                    if (obj instanceof Storable) {
                        obj.save(entities);
                        list.add(obj._id);
                    } else {
                        list.add(obj);
                    }
                });
                value = list;
            } else if (value instanceof Date) {
                value = new java.util.Date(value.getTime());
            }
            entity.put(id, value);
        }
        if (isRoot) {
            for(var entitiesIterator = entities.iterator(); entitiesIterator.hasNext();){
                var entity = entitiesIterator.next();
                getCollection(extractTypeFromKey(entity.get("_id"))).save(entity);
            }
        }
    };

    this.getEntity = function(type, arg) {
        if (isKey(arg)) {
            return getCollection(type).get(arg);
        } else if (isEntity(arg)) {
            return arg;
        } else if (arg instanceof Object) {
            var entity = new BasicDBObject();
            entity.put("_id", createKey(type, java.util.UUID.randomUUID().toString()));
            return entity;
        }
        return null;
    };

    this.getKey = function(type, arg) {
        if (isEntity(arg)) {
            return arg.get('_id');
        } else if (isKey(arg)) {
            return arg;
        }
        return null;
    };

    this.getId = function(key) {
        return isKey(key) ? key : null;
    };

    this.getProperties = function(store, entity) {
        var props = {};
        var map;
        if(isStorable(entity)){
            map = entity;
        } else {
            map = new ScriptableMap(entity.toMap());
        }
        for (var i in map) {
            var value = map[i];
            if (isKey(value) && i != "_id") {
                value = create(extractTypeFromKey(value), value);
            } else if (value instanceof java.util.List) {
                var array = [];
                for (var it = value.iterator(); it.hasNext(); ) {
                    var obj = it.next();
                    array.push(isKey(obj) ?
                               create(extractTypeFromKey(obj), obj) : obj);
                }
                value = array;
            }  else if (value instanceof java.util.Date) {
                value = new Date(value.getTime());
            } else {
                value = org.mozilla.javascript.Context.javaToJS(value, global);
            }
            props[i] = value;
        }
        return props;
    };

    this.equalKeys = function(keyA, keyB){
        return keyA == keyB;
    };

    function getCollection(type) {
        var collection = collections[type];
        if (!collection) {
            collection = collections[type] = new Collection(type);
        }
        return collection;
    }

    function Collection(type, options) {
        var collectionName = type;
        var collection = openCollection(db, collectionName, options);

        function openCollection(db, name, options){
            var dbconf = new BasicDBObject();
            var optionKeys = ["capped", "size", "max"];
            options = options || {};
            for(var o in optionKeys){
                if(options[optionKeys[o]]){
                    dbconf.put(optionKeys[o], options[optionKeys[o]]);
                }
            }
            var collection = db.getCollection(name);
            if(collection){
                return collection;
            } else {
                return db.createCollection(name, dbconf);
            }
        }

        this.all = function(){
            return this.query();
        }

        this.get = function(id){
            var docs = this.query(new BasicDBObject("_id", id));
            return docs.length ? docs[0] : null;
        }

        this.query = function(query, options, property){
            options = options || {};
            var list = [];
            var cursor;
            if(property){
                cursor = skipLimitAndSortCursor(collection.find(query, new BasicDBObject(property, 1)), options.limit, options.offset, options.orderings);
                while(cursor.hasNext()){
                    list.push(cursor.next().get(property));
                }
            } else {
                if(query){
                    cursor = skipLimitAndSortCursor(collection.find(query), options.limit, options.offset, options.orderings);
                } else {
                    cursor = skipLimitAndSortCursor(collection.find(), options.limit, options.offset, options.orderings);
                }
                while(cursor.hasNext()){
                    var doc = cursor.next();
                    var entity = create(type, doc.get("_id"), doc);
                    list.push(property ? entity[property] : entity);
                }
            }
            return list;
        }

        function skipLimitAndSortCursor(cursor, limit, offset, orderings){
            if(limit){
                cursor.limit(limit);
            }
            if(offset){
                cursor.skip(offset);
            }
            if(orderings){
                for(var o in orderings){
                    cursor.sort(orderings[o]);
                }
            }
            return cursor;
        }

        this.remove = function(id){
            collection.remove(new BasicDBObject("_id", id));
        }

        this.save = function(entity){
            collection.update(new BasicDBObject("_id", entity.get("_id")), entity, true, false);
        }
    }

    function BaseQuery(type) {
        var query = new BasicDBObject();
        var options = {orderings : []};

        this.type = type;

        this.select = function(property) {
            return getCollection(type).query(query, options, property);
        };

        this.equals = function(property, value) {
            query.put(property, convertJsValueToJava(value));
            return this;
        };

        this.greater = function(property, value) {
            query.put(property, new BasicDBObject("$gt",
                    convertJsValueToJava(value)));
            return this;
        };

        this.greaterEquals = function(property, value) {
            query.put(property, new BasicDBObject("$gte",
                    convertJsValueToJava(value)));
            return this;
        };

        this.less = function(property, value) {
            query.put(property, new BasicDBObject("$lt",
                    convertJsValueToJava(value)));
            return this;
        };

        this.lessEquals = function(property, value) {
            query.put(property, new BasicDBObject("$lte",
                    convertJsValueToJava(value)));
            return this;
        };

        this.orderBy = function(expression) {
            // ascending = +1
            // descending = -1
            options.orderings.push(
                    /^-.+$/.test(expression) ?
                    new BasicDBObject(expression.substring(1), -1) :
                    new BasicDBObject(expression, 1)
            );
            return this;
        };

        this.limit = function(value) {
            options.limit = value;
            return this;
        };

        this.offset = function(value) {
            validateOffsetValue(value);
            options.offset = value;
            return this;
        };

        this.range = function(from, to) {
            validateOffsetValue(from);
            options.offset = from;
            options.limit = to - from + 1;
            return this;
        };

        var validateOffsetValue = function(value) {
            if (value < 0) {
                throw new Error("Offset value mustn't be below zero.");
            }
        };
    }

    function openMongoDb(host, port, dbName, options) {
        return new Mongo(host, port).getDB(dbName);
    }

    function isEntity(value) {
        return value instanceof DBObject;
    }

    function extractTypeFromKey(key){
        return key.split(":")[0];
    }

    function createKey(type, id){
        return type + ":" + id;
    }

    function isKey(value) {
        return typeof value == "string" && value.contains(":") && registry[extractTypeFromKey(value)];
    }

    function isStorable(value) {
        return value instanceof Storable;
    }

}

// Helper used for query value conversion.
function convertJsValueToJava(value) {
    if (value instanceof Date) {
        return new java.util.Date(value.getTime());
    }
    return value;
}
