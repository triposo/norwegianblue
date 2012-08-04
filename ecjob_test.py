import sys
import os
from mockito import mock, verify

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from norwegianblue.ec2job import Future, Host
from norwegianblue import flags

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


def test_host_terminates_when_stopped():
  flags.non_flag_components()
  job = mock()
  connection = mock()
  spot_instance_request = mock()
  h = Host(job, connection, spot_instance_request)
  h._instance = mock()
  h._instance.state = 'running'
  h.run = lambda cmd: 0
  h.start()
  h.stop()
  h.join()
  verify(h._instance).terminate()
