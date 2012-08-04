import collections
import threading
import traceback

class Future(object):
  def __init__(self):
    self._value_set_event = threading.Event()
    self._value = None
    self._on_completion_callables = []


  def _trigger_completion(self):
    self._value_set_event.set()
    for c in self._on_completion_callables:
      c(self)


  def get(self):
    self._value_set_event.wait()
    if self._exception:
      raise self._exception
    return self._value


  @property
  def completed(self):
    return self._value_set_event.is_set()


  def set(self, value):
    if self.completed:
      return
    self._value = value
    self._trigger_completion()


  def set_error(self, exception):
    if self.completed:
      return
    self._exception = exception
    self._trigger_completion()


  def on_completion(self, callable):
    self._on_completion_callables.append(callable)


  @classmethod
  def wait_all(cls, futures):
    """
    Wait for all futures to get set. Ignore any errors.
    """
    for future in futures:
      try:
        future.get()
      except:
        pass


class BlockingQueue(object):
  def __init__(self, max_length=None):
    self._queue_changed_condition = threading.Condition()
    self._queue = collections.deque()
    self._max_length = max_length


  def take(self):
    with self._queue_changed_condition:
      while len(self._queue) == 0:
        self._queue_changed_condition.wait()
      entry = self._queue.popleft()
      self._queue_changed_condition.notifyAll()
      return entry


  def put(self, o):
    with self._queue_changed_condition:
      while self._max_length and len(self._queue) >= self._max_length:
        self._queue_changed_condition.wait()
      self._queue.append(o)
      self._queue_changed_condition.notifyAll()


class ThreadPool(object):

  class Thread(threading.Thread):
    def __init__(self, queue):
      super(ThreadPool.Thread, self).__init__()
      self.daemon = True
      self._queue = queue
      self._stopped = False


    def run(self):
      while not self._stopped:
        task = self._queue.take()
        task.execute()


    def stop(self):
      self._stopped = True

      
  class Task(object):
    def __init__(self, pool, callable, args=[], retries=5, future=Future()):
      self._callable = callable
      self._args = args
      self._retries_left = retries
      self._result_future = future
      self._pool = pool


    def execute(self):
      try:
        self._result_future.set(self._callable(*self._args))
      except StandardError as e:
        self.handle_failure(e)


    def handle_failure(self, exception):
      traceback.print_exc()
      if self._retries_left > 0:
        self._retries_left -= 1
        self._pool.enqueue(self)
      else:
        self._result_future.set_error(exception)


    @property
    def future(self):
      return self._result_future


    def get(self):
      return self._result_future.get()


    @property
    def completed(self):
      return self._result_future.completed


    def __repr__(self):
      return '<%s %s(%s) completed=%s>' %\
             (self.__class__.__name__, self._callable.__name__, repr(self._args), self._result_future.completed)

    
  def __init__(self, name='default', size=10):
    self._name = name
    self._size = size
    self._queue = BlockingQueue()
    self._threads = []
    self._started = False


  def start(self):
    if self._started:
      return
    self._started = True
    for i in range(self._size):
      t = ThreadPool.Thread(self._queue)
      self._threads.append(t)
      t.start()


  def stop(self):
    for t in self._threads:
      t.stop()


  def join(self):
    for t in self._threads:
      t.join()


  def enqueue(self, *args, **kwargs):
    self.start()
    task = ThreadPool.Task(self, *args, **kwargs)
    self._queue.put(task)
    return task

