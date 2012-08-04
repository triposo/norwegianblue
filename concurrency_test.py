import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from norwegianblue.concurrency import Future

def test_future_is_not_complete():
  f = Future()
  assert not f.completed

ran = False

def test_future_runs_on_completion():
  f = Future()

  def callback(f):
    global ran
    ran = True

  assert not ran
  f.on_completion(callback)
  f.set('')
  assert ran
