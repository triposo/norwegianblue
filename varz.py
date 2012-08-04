from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from contextlib import contextmanager
from threading import Thread

from norwegianblue import flags

flags.varz_port = flags.Int(9999)

class Variable(object):
  def value(self):
    raise StandardError("Must be overridden.")


class ExportedObject(Variable):
  def __init__(self, o):
    self._object = o

  def value(self):
    return repr(self._object)

  @property
  def object(self):
    return self._object


class ExportedFunction(Variable):
  def __init__(self, callable):
    self._callable = callable


  def value(self):
    return repr(self._callable())


class Counter(Variable):
  def __init__(self, value=0):
    self._value = value


  def value(self):
    return str(self._value)


  def increment(self, delta=1):
    self._value += delta


  def decrement(self, delta=1):
    self._value -= delta


  @contextmanager
  def incremented_value(self, increment=1):
    self.increment(increment)
    yield
    self.decrement(increment)


class VarzHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.send_header('Content-type', 'text/plain')
    self.end_headers()
    for name, value in globals().iteritems():
      if isinstance(value, Variable):
        self.wfile.write("%s=%s\n" % (name, value.value()))

  def log_message(self, format, *args):
    pass


def start():
  def run():
    server = HTTPServer(('', flags.varz_port), VarzHandler)
    server.serve_forever()

  t = Thread(target=run)
  t.daemon = True
  t.start()
