#!/usr/bin/python

# Bunch of utility functions.
import fnmatch
import hashlib
import operator
import os
import re
import struct
import threading
import thread
import gzip
import shutil
import bz2

from htmlentitydefs import name2codepoint

RE_HTML_ENTITIES = re.compile(r'&(#?[A-Za-z0-9]+?);')

def fingerprint(s):
  if not isinstance(s, basestring):
    s = repr(s)
  return abs(struct.unpack('i', hashlib.md5(s).digest()[:4])[0])


def shard_for_key(key, shards):
  return fingerprint(key) % shards


def filename_for_shard(fn, shards, index):
  if '.' in fn:
    fn, ext = fn.rsplit('.', 1)
    ext = '.' + ext
  else:
    ext = ''
  return '%s-%04d-%04d%s' % (fn, index, shards, ext)


def shard_names(spec):
  if '@' in spec:
    fname, shards = spec.split('@', 1)
    shards = int(shards)
    return [filename_for_shard(fname, shards, shard) for shard in range(shards)]
  else:
    return [spec]

def walk_pattern(curdir, spec):
  res = []
  for fn in os.listdir(curdir):
    if fnmatch.fnmatch(fn, spec[0]):
      full_fn = os.path.join(curdir, fn)
      if len(spec) > 1 and os.path.isdir(full_fn):
        res += walk_pattern(full_fn, spec[1:])
      else:
        if fn.lower().endswith('.dst'):
          res.append(full_fn.rsplit('.', 1)[0])
  return res

def expand_spec(spec):
  if ',' in spec:
    res = []
    for subspec in spec.split(','):
      res += expand_spec(subspec)
    return res
  if '*' in spec or '?' in spec:
    spec = spec.split(os.path.sep)
    return walk_pattern('.', spec)
  else:
    return shard_names(spec)


def replace_entities(match):
  try:
    ent = match.group(1)
    if ent[0] == "#":
      if ent[1] == 'x' or ent[1] == 'X':
        return unichr(int(ent[2:], 16))
      else:
        return unichr(int(ent[1:], 10))
    return unichr(name2codepoint[ent])
  except:
    return match.group()


def html_unescape(data):
  data = Unicode(data)
  return RE_HTML_ENTITIES.sub(ReplaceEntities, data)


def move_store(src, dst):
  if os.path.isfile(src):
    shutil.move(src, dst)
  else:
    shutil.move(src + '.dst', dst + '.dst')
    shutil.move(src + '.idx', dst + '.idx')


def move_sharded(src, dst):
  src = expand_spec(src)
  dst = expand_spec(dst)
  if len(src) != len(dst):
    raise ValueError('moving shared with unequal shard sizes (%s -> %s)' % (src, dst))
  for s, d in zip(src, dst):
    move_store(s, d)


def tokenize(what, lowercase=True):
  if what == []:
    return what
  if type(what) == list:
    return reduce(operator.add, [tokenize(x) for x in what])
  else:
    if lowercase:
      what = what.lower()
    return [x for x in re.split('[^a-zA-Z0-9]', what) if x]


EXTENSION_HANDLERS = {'gz': gzip.GzipFile,
                      'gzip': gzip.GzipFile,
                      'bz2': bz2.BZ2File,
                      }

def open_by_extension(filename, mode='r'):
  """Return a file like object based on the extension of the file.
  
  If the specified file does not exist, but a file exists with
  a known extension appended to it, return that one.
  
  Currently only gzip is supported."""
  if not os.path.isfile(filename):
    for ext, handler in EXTENSION_HANDLERS.items():
      with_ext = filename + '.' + ext
      if os.path.isfile(with_ext):
        return handler(with_ext, mode)
  dummy, ext = os.path.splitext(filename)
  if ext and ext[0] == '.':
    ext = ext[1:]
  handler = EXTENSION_HANDLERS.get(ext)
  if handler:
    return handler(filename, mode)
  return file(filename, mode)


class ParallelCall(object):
  def __init__(self, function, *args, **kargs):
    self._done = False
    self._result = None
    self._exception = None
    self._event = threading.Event()
    self._thread = thread.start_new_thread(self._run, (function, args, kargs))

  def _run(self, function, args, kargs):
    self._event.clear()
    try:
      self._result = apply(function, args, kargs)
    except Exception as self._exception:
      pass
    self._done = True
    self._event.set()

  def __call__(self):
    self._event.wait()
    if self._exception:
      raise self._exception
    return self._result


class Call(object):
  def __init__(self, function, *args, **kargs):
    self._function = function
    self._args = args
    self._kargs = kargs

  def __call__(self):
    return apply(self._function, self._args, self._kargs)


def unicode(st):
  if type(st) != unicode:
    return st.decode('utf-8', 'replace')
  return st


