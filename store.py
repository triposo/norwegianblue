#!/usr/bin/python

# Datastore: store data
import cPickle
import operator
import glob
import os
import struct
import zlib
import pymongo

import base

FORMAT = 'Datastore'
VERSION = 0.1


class DataStoreError(Exception):
  pass


class Serializer(object):
  def load(self, fd):
    raise NotImplemented

  def dump(self, fd, obj):
    raise NotImplemented


class PickleSerializer(Serializer):
  def load(self, fd):
    return cPickle.Unpickler(fd).load()

  def dump(self, fd, obj):
    cPickle.Pickler(fd, -1).dump(obj)


class ZLibPickleSerializer(Serializer):
  def load(self, fd):
    buf = fd.read(4)
    length = struct.unpack('I', buf)[0]
    buf = fd.read(length)
    s = zlib.decompress(buf)
    return cPickle.loads(s)

  def dump(self, fd, obj):
    s  = cPickle.dumps(obj)
    buf = zlib.compress(s)
    length = struct.pack('I', len(buf))
    fd.write(length)
    fd.write(buf)



PICKLERS = {None: PickleSerializer,
           'gzip': ZLibPickleSerializer}

class SingleStore(object):
  def __init__(self, fname, mode='r', compression=None, buffering=-1):
    self._open = False
    self._fname = fname
    self._mode = mode
    file_mode = mode
    if mode == 'a':
      file_mode += '+'
    file_mode += 'b'
    self._datafile = file(fname + '.dst', file_mode, buffering=buffering)
    if mode in ['a', 'r'] and os.path.isfile(fname + '.idx'):
      self._index = None
      self._index_file_name = fname + '.idx'
    else:
      self._index = {}
    self._open = True
    if mode == 'r':
      self.read_header(compression)
    elif mode == 'a':
      p = self._datafile.tell()
      self._datafile.seek(0)
      self.read_header(compression)
      self._datafile.seek(p)
    elif mode == 'w':
      self.write_header(compression)
    self._serializer = PICKLERS[self._compression]()

  def read_header(self, requested_compression):
    serializer = PICKLERS[None]()
    self._header = serializer.load(self._datafile)
    compression = self._header.get('compression')
    if requested_compression and requested_compression != compression:
      raise DataStoreError('%s compression asked, but store is in %s compression' % (requested_compression, compression))
    self._version = self._header['version']
    self._compression = compression

  def write_header(self, compression):
    header = {'format': FORMAT,
              'version': VERSION,
              'compression': compression}
    self._compression = compression
    serializer = PICKLERS[None]()
    serializer.dump(self._datafile, header)

  def read_index_if_needed(self):
    if self._index is None:
      self._index = self._serializer.load(file(self._index_file_name, 'rb'))

  def close(self):
    if self._open:
      self._open = False
      self.flush()
      self._datafile.close()

  def flush(self):
    if self._mode in ['a', 'w']:
      if not self._index is None:
        self._serializer.dump(file(self._fname + '.idx', 'wb'), self._index)
      self._datafile.flush()

  def items(self):
    if self._mode != 'r':
      raise Exception('Should be read mode')
    self._datafile.seek(0)
    self.read_header(self._compression)
    while True:
      try:
        key = self._serializer.load(self._datafile)
        val = self._serializer.load(self._datafile)
      except EOFError:
        break
      yield key, val

  def values(self):
    for k, v in self.items():
      yield v

  def __len__(self):
    self.read_index_if_needed()
    return len(self._index)

  def __setitem__(self, key, value):
    self.read_index_if_needed()
    if key in self._index:
      raise KeyError('Can only set a key once (%s)' % key)
    self._index[key] = self._datafile.tell()
    self._serializer.dump(self._datafile, key)
    self._serializer.dump(self._datafile, value)

  def __getitem__(self, key):
    self.read_index_if_needed()
    self._datafile.seek(self._index[key])
    key2 = self._serializer.load(self._datafile)
    if key2 != key:
      raise DataStoreError('Inconsistent store')
    return self._serializer.load(self._datafile)

  def __del__(self):
    self.close()

  def __contains__(self, key):
    self.read_index_if_needed()
    return key in self._index

  def keys(self):
    self.read_index_if_needed()
    return self._index.keys()

  def get(self, key, default=None):
    self.read_index_if_needed()
    if key in self._index:
      return self[key]
    else:
      return default

  def NumShards(self):
    return 1


class MultiStore(SingleStore):
  """MultiStore is a DataStore that allows multiple values per key. It is not a perfect match."""

  def __len__(self):
    self.read_index_if_needed()
    return [len(v) for v in self._index.values()]

  def __setitem__(self, key, value):
    self.read_index_if_needed()
    self._index.setdefault(key, []).append(self._datafile.tell())
    self._serializer.dump(self._datafile, key)
    self._serializer.dump(self._datafile, value)

  def __getitem__(self, key):
    res = []
    self.append_values_at(key, res)
    return res

  def append_values_at(self, key, res):
    for v in self.iterate_values_at(key):
      res.append(v)
  
  def iterate_values_at(self, key):
    self.read_index_if_needed()
    lst = self._index.get(key)
    if not lst:
      return
    for pos in lst:
      self._datafile.seek(pos)
      key2 = self._serializer.load(self._datafile)
      if key2 != key:
        raise DataStoreError('Inconsistent store')
      yield self._serializer.load(self._datafile)


class Store(object):
  """A sharded version of the single store. Probably the one you want to use.
  
  A store resolves a filename like store@shards into a series of sub store.
  If no shards are specified and store exist, open a single shard store. If
  store does not exist as file, try to deduce shards from files that do exist.
  """
  def __init__(self, fname, shards=None, mode='r', DataStoreType=SingleStore, compression=None, buffering=-1):
    self._shards = []
    if shards is None:
      if '@' in fname:
        basename, shards = fname.split('@', 1)
        shards = int(shards)
      else:
        basename = fname
        if not os.path.exists(basename):
          if basename.endswith('-'):  # could be of shell expansion
            basename = basename[:-1]
          existing_file = glob.glob(basename +'-????-????.dst')
          if existing_file:
            shards = int(existing_file[0][len(basename) + 6:len(basename) + 10])
          else:
            basename = fname
    if shards is None:
      self._shards = [DataStoreType(basename, mode=mode, compression=compression, buffering=-1)]
    else:
      self._shards = [DataStoreType(base.filename_for_shard(basename, shards, shard),
                                    mode=mode,
                                    compression=compression,
                                    buffering=-1)
                      for shard in range(shards)]

  def close(self):
    for shard in self._shards:
      shard.close()

  def flush(self):
    for shard in self._shards:
      shard.flush()

  def values(self):
    for shard in self._shards:
      for v in shard.values():
        yield v

  def items(self):
    for shard in self._shards:
      for v in shard.items():
        yield v

  def __len__(self):
    return sum([len(x) for x in self._shards])

  def __setitem__(self, key, value):
    idx = base.shard_for_key(key, len(self._shards))
    self._shards[idx][key] = value

  def __getitem__(self, key):
    idx = base.shard_for_key(key, len(self._shards))
    return self._shards[idx][key]

  def get(self, key, default=None):
    idx = base.shard_for_key(key, len(self._shards))
    return self._shards[idx].get(key, default)

  def __del__(self):
    self.close()

  def __contains__(self, key):
    idx = base.shard_for_key(key, len(self._shards))
    return key in self._shards[idx]

  def keys(self):
    return reduce(operator.add, [x.keys() for x in self._shards])

  def num_shards(self):
    return len(self._shards)
  
  def get_shard(self, n):
    return self._shards[n]


class MongoStore(object):
  """A class implementing the store interface based on MongoDb.

  The store is mapped almost 1:1 on a mongo collection, but keeps automatically a
  fingeprint field and keeps meta information around about the last changed time.
  """
  TAG = 'mongo:'
  GEO_INDEX =[("loc", pymongo.GEO2D)]

  def __init__(self, spec, indexes=None, update=False):
    db_name, collection_name = MongoStore.parse_spec(spec)
    self.update = update
    self.db = pymongo.Connection()[db_name]
    self.collection = self.db[collection_name]
    if indexes:
      for index in indexes:
        self.collection.ensure_index(index)
    self.collection_name = collection_name

  @classmethod
  def parse_spec(cls, spec):
    if spec.startswith(MongoStore.TAG):
      spec = spec[len(MongoStore.TAG):]
    db_name, collection_name = spec.split('/')
    return db_name, collection_name

  @classmethod
  def move(cls, src, dest):
    src_db_name, src_collection_name = MongoStore.parse_spec(src)
    dest_db_name, dest_collection_name = MongoStore.parse_spec(dest)
    if src_db_name != dest_db_name:
      raise StandardError('can not move a collection across databases')
    db = pymongo.Connection()[src_db_name]
    db[src_collection_name].rename(dest_collection_name, dropTarget=True)

  @classmethod
  def exists(cls, spec):
    db_name, collection_name = MongoStore.parse_spec(spec)
    db = pymongo.Connection()[db_name]
    return collection_name in db.collection_names()

  def set_update(self, update):
    self.update = update

  def merge_into(self, dest):
    """Merge all values from src into dest."""
    dest_db_name, dest_collection_name = MongoStore.parse_spec(dest)
    if dest_db_name != self.collection.database.name:
      raise StandardError('can not merge_into across databases')
    import pymongo.code
    f = pymongo.code.Code("db['%s'].find().forEach("
                          "  function(obj) {"
                          "    var _id = obj._id;"
                          "    delete obj._id;"
                          "    db['%s'].update({_id: _id}, {'$set': obj});"
                          "  })" % (self.collection_name, dest_collection_name))
    self.collection.database.eval(f)

  @classmethod
  def delete(cls, spec):
    db_name, collection_name = MongoStore.parse_spec(spec)
    db = pymongo.Connection()[db_name]
    db.drop_collection(collection_name)

  def close(self):
    pass

  def flush(self):
    pass

  def values(self):
    return self.collection.find()

  def items(self):
    for value in self.collection.find():
      yield value['_id'], value

  def __len__(self):
    return self.collection.count()

  def __setitem__(self, key, value):
    d = dict(value)
    d['_id'] = key
    d['fp'] = base.fingerprint(key)
    if self.update:
      self.collection.update({'_id': key}, d, upsert=True)
    else:
      self.collection.insert(d)

  def __getitem__(self, key):
    return self.collection.find_one(key)

  def get(self, key, default=None):
    value = self.__getitem__(key)
    if value is None:
      return default
    return value

  def __del__(self):
    self.close()

  def __contains__(self, key):
    return not self.__getitem__(key) is None

  def keys(self):
    for value in self.collection.find():
      yield value['_id']


def open(spec, shards=None, mode='r', DataStoreType=SingleStore, compression=None, buffering=-1, mongo_indexes=None):
  """Utility function to open either a mongo collection or a store. If spec == '' or None a dummy will be returned."""
  if not spec:
    return {}
  elif spec.startswith(MongoStore.TAG):
    return MongoStore(spec, mongo_indexes)
  else:
    return Store(spec, shards, mode, DataStoreType, compression, buffering)
