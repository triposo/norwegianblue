import unittest
from test import test_support

import flags

flags. = flags.AllowUndefined()

import mapreduce
import store

from nose.plugins.skip import  SkipTest

class TestRecordIteration(unittest.TestCase):
  def setUp(self):
    pass

  def testXmlTask(self):
    raise SkipTest
    tmp = test_support.TESTFN
    file(tmp, 'w').write(
      '<xml?>\n'
      '<value atrr="1">\n'
      'bla\n'
      '</value>\n'
      '<value atrr="2">\n'
      'something a little longer!\n'
      '</value>\n'
      '<value atrr="3">\n'
      'Even longer. This and it has value in it.\n'
      '</value>\n'
    )

    work = mapreduce.input_tasks('xml:value:' + tmp, 2)
    v1s = [v for id, v in work.get_iterator(0).items()]
    v2s = [v for id, v in work.get_iterator(1).items()]
    self.assertEqual(3, len(v1s) + len(v2s))


class FakeStore:
  registered = {}

  @classmethod
  def register_path(cls, path, d):
    FakeStore.registered[path] = d

  def __init__(self, path, mode='r'):
    self._store = FakeStore.registered.setdefault(path, {})

  def close(self):
    pass

  def flush(self):
    pass

  def items(self):
    return self._store.items()

  def values(self):
    return _store.values()

  def __len__(self):
    return len(self._store)

  def __setitem__(self, key, value):
    self._store[key] = value

  def __getitem__(self, key):
    return self._store[key]

  def __del__(self):
    pass

  def __contains__(self, key):
    return key in self._store

  def keys(self):
    return self._store.keys()

  def get(self, key, default=None):
    return self._store.get(key, default)

  def NumShards(self):
    return 1


class RunMapreduce(unittest.TestCase):
  def setUp(self):
    self.save_store = store.Store
    store.Store = FakeStore
    self.save_single_store = store.SingleStore
    store.SingleStore = FakeStore

  def tearDown(self):
    store.Store = self.save_store
    store.SingleStore = self.save_single_store

  def test_identity(self):
    def mapper(key, value):
      yield key, value

    def reducer(key, values):
      for x in values:
        return x
      raise StopIteration

    FakeStore.register_path('input', {'key1': 'val1', 'key2': 'val2'})
    mapreduce.run(mapper, reducer, input_map='input', output_map='output')
    output = FakeStore.registered['output']
    self.assertEqual(2, len(output))
    self.assertEqual('val1', output['key1'])
    self.assertEqual('val2', output['key2'])

if __name__ == '__main__':
  unittest.main()
