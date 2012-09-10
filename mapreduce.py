#!/usr/bin/python

# poor men's mapreduce

import fnmatch
import os
import subprocess
import sys
import thread
import time
import types

import base
from norwegianblue import flags
import store

flags.input_map = flags.File('')
flags.input_debug_key = flags.String('', 'If set, only process this key.')
flags.grep_debug_key = flags.String('', 'If set, only process keys matching this.')
flags.output_map = flags.File('')
flags.work_dir = flags.File('/tmp')
flags.remove_state = flags.Bool(True)
flags.parallelism = flags.Int(1, 'If set to >1 will spawn subprocesses to do work.')
flags.perform_task = flags.String('',
    'Perform one or more tasks. Used internally. Format is [map|reduce]:taskid:taskid..')
flags.log_every = flags.Int(1000)
flags.input_map_buffering = flags.Int(-1)
flags.record_limit = flags.Int(0)
flags.fp_restart_overwrite = flags.String('')
flags.use_previous = flags.Bool(False,
    'To be used in a pipeline setting. If set to true, before attempting to calculate the new value'
    ' use existing values.')
flags.run_parallel = flags.Bool(False)

counts = {}
def count(key, increment=1):
  """Increase the count of key."""
  counts[key] = counts.get(key, 0) + increment

def count_line(marker='}}'):
  return marker + ' '.join(['%s=%s' % v for v in counts.items()])

def shard_names(spec):
  if '@' in spec:
    fname, shards = spec.split('@', 1)
    shards = int(shards)
    return [base.filename_for_shard(fname, shards, shard) for shard in range(shards)]
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

def input_shard_names(spec):
  if ',' in spec:
    res = []
    for subspec in spec.split(','):
      res += input_shard_names(subspec)
    return res
  if '*' in spec or '?' in spec:
    spec = spec.split(os.path.sep)
    return walk_pattern('.', spec)
  else:
    return shard_names(spec)

def tmpfile(fn):
  if flags.fp_restart_overwrite:
    fn = flags.fp_restart_overwrite + fn
  else:
    fn = flags.StateFp() + fn
  return os.path.join(flags.work_dir, fn)


class TaskProcessor(object):
  def num_tasks(self):
    """Return the number of tasks that the job can be split into."""
    return 1
  
  def iterate_key_values(self, idx, debug_key=None):
    """Iterate through the records for task idx. If debug_key is set, return only the value for that key."""
    raise NotImplementedError

  def set_filter(self, filter):
    self.filter = filter


class StoreTask(TaskProcessor):
  def __init__(self, spec, _):
    self.shard_names = input_shard_names(spec)

  def num_tasks(self):
    return len(self.shard_names)
  
  def iterate_key_values(self, idx, debug_key=None):
    st = store.SingleStore(self.shard_names[idx], buffering=flags.input_map_buffering)
    if debug_key:
      if debug_key in st:
        yield debug_key, st[debug_key]
      elif debug_key.isdigit() and int(debug_key) in st:
        yield debug_key, st[int(debug_key)]
      else:
        raise StopIteration
    else:
      for key, value in st.items():
        yield key, value


class MongoTask(TaskProcessor):
  def __init__(self, spec, parallelism):
    self.parallelism = parallelism
    self.collection = store.MongoStore(spec).collection

  def num_tasks(self):
    return self.parallelism

  def iterate_key_values(self, idx, debug_key=None):
    if debug_key:
      debug_val = self.collection.find_one({'_id': debug_key})
      if debug_val:
        yield debug_key, debug_val
      elif debug_key.isdigit():
        debug_val = self.collection.find_one({'_id': int(debug_key)})
        if debug_val:
          yield debug_key, debug_val
      raise StopIteration
    else:
      if self.filter:
        filter = dict(self.filter)
      else:
        filter = {}
      filter['fp'] = {'$mod': [self.parallelism, idx]}
      for value in self.collection.find(filter):
        yield value['_id'], value


class RangeTask(TaskProcessor):
  def __init__(self, spec, parallelism):
    if '-' in spec:
      start, end = spec.split('-', 1)
    else:
      start = 0
      end = spec
    self.start = int(start)
    self.end = int(end)
    self.parallelism = parallelism

  def num_tasks(self):
    return self.parallelism

  def iterate_key_values(self, idx, debug_key=None):
    task_size = (self.end - self.start) / self.parallelism
    start = task_size * idx
    end = self.end if idx == self.parallelism -1 else task_size * (idx + 1)
    print 'range:%d-%d' % (start, end)
    if debug_key:
      if start <= debug_key < end:
        yield debug_key, debug_key
      else:
        raise StopIteration
    else:
      for key in range(start, end):
        yield key, key


class LinesRecordTask(TaskProcessor):
  """The LinesRecordTask deals with the common situation where the input is made out
  of records spread out over multiple lines that are somehow separated. The input
  might be compressed so we just read the whole input for all tasks round robbining
   the work.

   The standard implementation treats every line as an end record marker and therefore
   has one record per line. By overriding marks_end_record(line) this behavior can be
   changed for example to return only true for the end xml tag.

   Mappers will be called with key==index in the file of the record and value a list
   of lines belonging to this record.
  """
  def __init__(self, spec, parallelism):
    self.input_file = spec
    self.parallelism = parallelism

  def num_tasks(self):
    return self.parallelism

  def marks_end_record(self, line):
    return True

  def iterate_key_values(self, idx, debug_key=None):
    count = 0
    buffer = []
    for line in base.open_by_extension(self.input_file):
      buffer.append(line)
      if self.marks_end_record(line):
        if count % self.parallelism == idx:
          if not debug_key or debug_key == count:
            yield count, buffer
        count += 1
        buffer = []


class XmlTask(LinesRecordTask):
  def __init__(self, spec, parallelism):
    self.tag, spec = spec.split(':', 1)
    self.tag = '</%s>' % self.tag
    super(XmlTask, self).__init__(spec, parallelism)

  def marks_end_record(self, line):
    return line.strip() == self.tag


task_map = {'store': StoreTask,
            'range': RangeTask,
            'lines': LinesRecordTask,
            'mongo': MongoTask,
            'xml': XmlTask}

def install_task_procesor(name, taskprocessor):
  """Add a task processor under the specified name.
  
  Define a new mapreduce input specifier. The taskprocessor should be a
  class implementing the TaskProcessor "interface".
  """
  task_map[name] = taskprocessor

def input_tasks(spec, parallelism, filter):
  if ':' in spec:
    type, spec = spec.split(':', 1)
  else:
    type = 'store'
  res = task_map[type](spec, parallelism)
  res.set_filter(filter)
  return res


class OutputWriter(object):
  def write(self, key, value):
    raise NotImplementedError

  def __setitem__(self, key, value):
    self.write(key, value)

  def close(self):
    pass


class StoreWriter(OutputWriter):
  def __init__(self, spec):
    self.store = store.SingleStore(spec, mode='w')

  def write(self, key, value):
    self.store[key] = value

  def close(self):
    self.store.close()


class MongoWriter(StoreWriter):
  def __init__(self, spec):
    self.store = store.MongoStore(spec)

  def write(self, key, value):
    self.store[key] = value

  def set_update_mode(self, update_mode):
    self.store.set_update(update_mode)


class TextWriter(OutputWriter):
  def __init__(self, spec):
    self.text_file = file(spec, 'w')

  def write(self, key, value):
    self.text_file.write('%s\t%s\n' % (key, value))

  def close(self):
    self.text_file.close()



writer_map = {'store': StoreWriter,
              'text': TextWriter,
              'mongo': MongoWriter}

def install_writer(name, writer):
  """Add a output writer class under the specified name."""
  writer_map[name] = writer


def get_writer_from_spec(spec):
  if ':' in spec:
    type, spec = spec.split(':', 1)
  else:
    type = 'store'
  return writer_map[type](spec)


class Mapper(object):
  def __init__(self, task_ids, shards):
    self.task_ids = task_ids
    self.shards = shards

  def attach_writer(self, writer):
    """Do any preparations to the output writer before we're starting. Only called when skipping the reduce phase."""
    self.current_writer = writer

  def begin(self):
    """Called before this worker is going to do any work."""
    return ()

  def start_map(self, task_id):
    """Called when this worker is starting on task task_id"""
    self.task_id = task_id
    return ()

  def secondary_store(self, base_path, mode='r', compression=None):
    """Return a store that mimics the sharding of the input store and can be used as as secondary output/input."""
    if '@' in base_path:
      base_path, sharding = base_path.split('@')
      if int(sharding) != self.shards:
        raise StandardError('secondary_store sharding should be the same as input sharding. (%s vs %s' %
                            (int(sharding), self.shards))
    return store.SingleStore(base.filename_for_shard(base_path, self.shards, self.task_id),
                             mode=mode, compression=compression) 

  def process(self, key, val):
    yield key, val

  def flush(self, task_id):
    """Called when a specific map task is done."""
    return ()

  def finished(self):
    """Called when this process is done."""
    return ()

class FunctionMapper(Mapper):
  def __init__(self, function, task_ids, shards):
    super(FunctionMapper, self).__init__(task_ids, shards)
    self.function = function

  def process(self, key, val):
    return self.function(key, val)

class IdentityMapper(Mapper):
  def process(self, key, val):
    yield key, val


class Reducer(object):
  def __init__(self, task_id, shards):
    self.task_id = task_id
    self.shards = shards

  def attach_writer(self, writer):
    """Do any preparations to the output writer before we're starting."""
    pass

  def process(self, key, values):
    return list(values)


class IdentityReducer(Reducer):
  def process(self, key, values):
    return list(values)
  

class SumReducer(Reducer):
  def process(self, key, values):
    return sum(values)


class FirstReducer(Reducer):
  def process(self, key, values):
    for v in values:
      return v
    raise StopIteration


class SkipReducer(Reducer):
  """Specify this if you want to skip the reduce phase.

  Note when running against mongo any yields from the mapper will
  be applied directly as updates against the target, which allows
  in many cases to skip the reducer but still aggregate:

    yield _id, {'$inc': {photocount: 1}}

  Will increment the photcount of the target by one.

  """
  def process(self, key, values):
    raise NotImplementedError


def instantiate_mapper(mapper, task_ids, shards):
  if isinstance(mapper, types.FunctionType):
    res = FunctionMapper(mapper, task_ids, shards)
  else:
    res =  mapper(task_ids, shards)
  res.begin()
  return res


def instantiate_reducer(reducer, task_id, shards, ds_out):
  if isinstance(reducer, types.FunctionType):
    return reducer
  r = reducer(task_id, shards)
  r.attach_writer(ds_out)
  return r.process


def run_pipeline(mapper, input_map=None, output_map=None):
  """Use this when you don't need the reducer and input and output have the same keys."""
  flags.non_flag_components()
  if not input_map:
    input_map = flags.input_map
  if not output_map:
    output_map = flags.output_map
  input_names = input_shard_names(input_map)
  in_shard_num = len(input_names)
  output_names = shard_names(output_map)
  out_shard_num = len(output_names)

  if flags.use_previous:
    print 'using previous'
    if '@' in output_map:
      prev_map = output_map.replace('@', '.old@')
    else:
      prev_map = output_map + '.old'
    prev_shard_names = shard_names(prev_map)
    exists = True
    for prev, out in zip(prev_shard_names, output_names):
      for ext in ('dst', 'idx'):
        fn_old = '%s.%s' % (prev, ext)
        fn_new = '%s.%s' % (out, ext)
        if os.path.isfile(fn_old):
          os.remove(fn_old)
        if os.path.isfile(fn_new):
          os.rename(fn_new, fn_old)
        else:
          exists = False
    if exists:
      previous = store.Store(prev_map)
    else:
      previous = {}
      prev_shard_names = [None] * out_shard_num
  else:
    prev_shard_names = [None] * out_shard_num
    previous = {}
  
  if flags.run_parallel and out_shard_num == in_shard_num:
    import multiprocessing
    pool = multiprocessing.Pool(2)
    work = zip(range(in_shard_num),
               [mapper] * in_shard_num,
               input_names,
               output_names,
               prev_shard_names)
    pool.map(RunPipelineTask, work)
    return
    
  input_map = store.Store(input_map)
  output_map = store.Store(output_map, mode='w')
  count = 0
  cached = 0
  l = len(input_map.keys())
  for key, value in input_map.items():
    perc = int(100 * float(count) / l)
    if count % 100 == 0:
      print perc, '%', key, cached
    count += 1
    if key in previous:
      if previous_mapper:
        output_map[key] = previous_mapper(key, value, previous[key])
      else:
        output_map[key] = previous[key]
      cached += 1
    else:
      for k, v in mapper(key, value):
        output_map[k] = v
        break
  input_map.close()
  output_map.close()
  if flags.use_previous and previous:
    previous.close()
    for shard in shard_names(prev_map):
      for ext in ('dst', 'idx'):
        fn_prev = '%s.%s' % (prev, ext)
        if os.path.isfile(fn_prev):
          os.remove(fn_prev)
  print 'alldone'


def run_maptask(mapper, key_value_iterator, out_shards, markerfn, task_id):
  out_shard_num = len(out_shards)
  num_recs = 0

  for k, v in mapper.start_map(task_id):
    out_shards[base.shard_for_key(k, out_shard_num)][k] = v
    count('start-out')
  for key, value in key_value_iterator:
    if flags.grep_debug_key:
      if not flags.grep_debug_key in key:
        continue
      else:
        print "Processing: %s" % key
    if flags.log_every and num_recs % flags.log_every == 0:
      print count_line()
    count('map-in')
    num_recs += 1
    if flags.record_limit == num_recs:
      break
    for kv in mapper.process(key, value):
      if kv:
        k, v = kv
        out_shards[base.shard_for_key(k, out_shard_num)][k] = v
        count('map-out')
  for k, v in mapper.flush(task_id):
    out_shards[base.shard_for_key(k, out_shard_num)][k] = v
    count('flush-out')
  for out_shard in out_shards:
    if not out_shard is None:
      out_shard.close()
  file(markerfn, 'w').write('DONE')


def iterate_values_at(in_shards, k):
  for in_shard in in_shards:
    for v in in_shard.iterate_values_at(k):
      yield v


def run_reducetask(keys, in_shards, ds_out, reducer, markerfn, task_id, shards):
  reducer = instantiate_reducer(reducer, task_id, shards, ds_out)
  num_recs = 0
  for k in keys:
    if flags.log_every and num_recs % flags.log_every == 0:
      print '}}' + ' '.join(['%s=%s' % v for v in counts.items()])
    num_recs += 1
    try:
      count('reduce-out')
      ds_out.write(k, reducer(k, iterate_values_at(in_shards, k)))
    except StopIteration:
      pass
  ds_out.close()
  for in_shard in in_shards:
    in_shard.close()
  file(markerfn, 'w').write('DONE')


class Master:
  """Master runs the mapreduce for parallelism > 1.
  
  A seperate process is started per requested parallel instance
  and a thread is spawn to watch that process and merge the
  output coming back.
  """

  def __init__(self, parallelism,  input_tasks, reduce_shards, skip_reducer):
    self.parallelism = parallelism
    self.input_tasks = input_tasks
    self.reduce_shards = reduce_shards
    self.skip_reducer = skip_reducer
    self.running_tasks = 0
    self.script = ' '.join(sys.argv)

  def run_task(self, perform_task):
    # Tell the subtask what to do and 
    cmd = '%s -u %s --perform_task=%s --fp_restart_overwrite=%s' % (
        sys.executable, self.script, perform_task, flags.StateFp())
    p = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
    local_counts = {}
    while p.returncode is None:
      line = p.stdout.readline()
      if line:
        line = line.strip()
        if line.startswith('}}'):
          counts = line[2:].split(' ')
          for c in counts:
            if c:
              k, v = c.split('=', 1)
              v = int(v)
              prev = local_counts.get(k, 0)
              count(k, v - prev)
              local_counts[k] = v
          print count_line(marker='')
        else:
          print line
      p.poll()
    if p.returncode != 0:
      print 'task died:', perform_task
      self.error_code = p.returncode
    else:
      print 'task done:', perform_task
    self.running_tasks -= 1

  def run(self):
    phases = [('map', self.input_tasks.num_tasks())]
    if not self.skip_reducer:
      phases.append(('reduce', self.reduce_shards))
    for phase, num_shards in phases:
      print 'running %s phase' % phase
      perform_tasks = [phase] * self.parallelism
      for shard in range(num_shards):
        perform_tasks[shard % self.parallelism] += ':%s' % shard
      self.error_code = 0
      for perform_task in perform_tasks:
        if perform_task != phase:
          self.running_tasks += 1
          thread.start_new_thread(self.run_task, (perform_task,))
      while self.running_tasks > 0:
        time.sleep(0.1)
        if self.error_code != 0:
          sys.exit(self.error_code)

def is_master():
  """Return whether we're the master."""
  return flags.parallelism > 1 and not flags.perform_task


def run(mapper, reducer, input_map=None, output_map=None, compression=None, filter=None):
  flags.non_flag_components()

  if not mapper:
    mapper = IdentityMapper
  if not reducer:
    reducer = IdentityReducer
  if not input_map:
    input_map = flags.input_map
  if not output_map:
    output_map = flags.output_map

  input_source = input_tasks(input_map, flags.parallelism, filter)

  skip_reducer = (reducer == SkipReducer)

  if output_map.startswith(store.MongoStore.TAG):
    # TODO(Douwe): dont special case Mongo but let the Writer class decide
    output_names = [output_map] * flags.parallelism
  else:
    output_names = shard_names(output_map)
  out_shard_num = len(output_names)
  
  if is_master():
    # No specific task to run, but parallel. We are the master
    master = Master(flags.parallelism, input_source, out_shard_num, skip_reducer)
    master.run()
  else:
    if flags.perform_task:
      perform_task = True
      task_phase, task_ids = flags.perform_task.split(':', 1)
      task_ids = [int(id) for id in task_ids.split(':')]
    else:
      perform_task = False
      task_phase = None
      task_ids = range(input_source.num_tasks())
  
    if not perform_task:
      print 'Started map phase'
    
    if not perform_task or task_phase == 'map':
      mapper_instance = instantiate_mapper(mapper, task_ids, input_source.num_tasks())
      for task_id in task_ids:
        markerfn = tmpfile('MAP_%d_DONE' % task_id)
        if not os.path.isfile(markerfn):
          if skip_reducer:
            if output_map.startswith(store.MongoStore.TAG):
              shard_writer = get_writer_from_spec(output_names[0])
              mapper_instance.attach_writer(shard_writer)
              out_shards = [shard_writer]
            else:
              # We skip the reducer. To make sure the mapper behaves, i.e. only writes to the corresponding
              # shard, supply Nones
              if out_shard_num != input_source.num_tasks():
                raise StandardError('Need the same in and output sharding when skipping reducer.')
              out_shards = [None for out_idx in range(out_shard_num)]
              shard_writer = get_writer_from_spec(output_names[task_id])
              mapper_instance.attach_writer(shard_writer)
              out_shards[task_id] = shard_writer
          else:
            base_out = tmpfile('inter-%04d-%%04d' % task_id)
            out_shards = [store.MultiStore(base_out % out_idx, mode='w', compression=compression)
                          for out_idx in range(out_shard_num)]
          run_maptask(mapper_instance, input_source.iterate_key_values(task_id, flags.input_debug_key),
                      out_shards, markerfn, task_id)

    if (not perform_task or task_phase == 'reduce') and not skip_reducer:
      for task_id, ds_out_name in enumerate(output_names):
        if perform_task and not task_id in task_ids:
          continue
        markerfn = tmpfile('REDUCE_%d_DONE' % task_id)
        if not os.path.isfile(markerfn):
          ds_out = get_writer_from_spec(ds_out_name)
          base_in = tmpfile('inter-%%04d-%04d' % task_id)
          in_shards = [store.MultiStore(base_in % in_task_id) for in_task_id in range(input_source.num_tasks())]
          keys = set()
          for in_shard in in_shards:
            keys.update(set(in_shard.keys()))
          keys = list(keys)
          keys.sort()
          run_reducetask(keys, in_shards, ds_out, reducer, markerfn, task_id, len(output_names))

  if flags.remove_state:
    for fn in os.listdir(flags.work_dir):
      if fn.startswith(flags.StateFp()):
        os.remove(os.path.join(flags.work_dir, fn))

  print count_line()

if __name__ == '__main__':
  run(IdentityMapper, FirstReducer)
