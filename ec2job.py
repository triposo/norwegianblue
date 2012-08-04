import collections
from contextlib import closing, contextmanager
import datetime
import subprocess
import tarfile
import threading
import traceback
import boto
import os
import sys
import time
import paramiko

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from norwegianblue import flags
from norwegianblue import varz
from norwegianblue.concurrency import Future, BlockingQueue
import utils


flags.logs = flags.File('logs')
flags.skip_init_hosts = flags.Bool(False)
flags.terminate_hosts = flags.Bool(True)
flags.availability_zone_group = flags.Bool('us-east-1c')

varz.task_queue = varz.ExportedObject([])
varz.task_queue_length = varz.ExportedFunction(lambda: len(varz.task_queue.object))
varz.tasks_enqueued_count = varz.Counter()
varz.tasks_completed_count = varz.Counter()

class Task(object):
  def __init__(self, callable, args=[], retries=5, future=Future()):
    self._callable = callable
    self._args = args
    self._retries_left = retries
    self._result_future = future


  def execute(self, job, host):
    try:
      self._result_future.set(self._callable(host, *self._args))
    except StandardError as e:
      self.handle_failure(job, host, e)


  def handle_failure(self, job, host, exception):
    traceback.print_exc()
    if self._retries_left > 0:
      self._retries_left -= 1
      job.execute_on_next_free_host(self)
    else:
      self._result_future.set_error(exception)


  @property
  def future(self):
    return self._result_future


  def __repr__(self):
    return '<%s %s(%s) completed=%s>' %\
           (self.__class__.__name__, self._callable.__name__, repr(self._args), self._result_future.completed)


class Job(object):
  def __init__(self, worker_count, price, image_id, key_name, instance_type, security_groups):
    self._free_hosts = BlockingQueue()
    self.price = price
    self.worker_count = worker_count
    self.image_id = image_id
    self.key_name = key_name
    self.instance_type = instance_type
    self.security_groups = security_groups
    self._ec2 = None
    self._hosts = []
    varz.hosts = varz.ExportedObject(self._hosts)
    varz.hosts_count = varz.ExportedFunction(lambda: len(self._hosts))
    varz.hosts_working_count = varz.ExportedFunction(lambda: len([h for h in self._hosts if h.status == 'working']))


  def connection(self):
    if not self._ec2:
      print 'Connecting to EC2'
      aws_access_key, aws_secret_key = utils.read_awssecret()
      self._ec2 = boto.connect_ec2(aws_access_key, aws_secret_key)
    return self._ec2


  def add_workers(self, count):
    if count > 0:
      print 'Adding %d new spot requests in serverset %s' % (count, flags.serverset)
      valid_until = datetime.datetime.utcnow() + datetime.timedelta(days=flags.valid_days)
      new_requests = self.connection().request_spot_instances(
        price=self.price, image_id=self.image_id, count=count,
        security_groups=self.security_groups, valid_until=valid_until.isoformat(),
        key_name=self.key_name, instance_type=self.instance_type,
        availability_zone_group=flags.availability_zone_group)
      for r in new_requests:
        r.add_tag('serverset', flags.serverset)
      print 'New spot requests %s' % (','.join(r.id for r in new_requests))
      # Kick off controller threads for new instances.
      for req in new_requests:
        host = Host(self, self.connection(), req)
        self._hosts.append(host)
        host.start()
      return new_requests
    else:
      return []


  def start(self):
    def run():
      # Find existing requests in serverset.
      # TODO(tirsen): Support non-spot instances.
      active_requests = self.connection().get_all_spot_instance_requests(
        filters={'tag:serverset': flags.serverset, 'state': 'active'})
      open_requests = self.connection().get_all_spot_instance_requests(
        filters={'tag:serverset': flags.serverset, 'state': 'open'})
      existing_requests = active_requests + open_requests
      print 'Found %d existing spot requests in serverset %s' % (len(existing_requests), flags.serverset)
      # Kick off controller threads for existing instances.
      for req in existing_requests:
        host = Host(self, self.connection(), req)
        self._hosts.append(host)
        host.start()

      # Request new instances.
      new_requests = self.add_workers(self.worker_count - len(existing_requests))

      # Run the actual job.
      try:
        self.run()
      except StandardError:
        traceback.print_exc()

      # Terminate all the instances and wait until they are done.
      print "Done. Stopping all hosts."
      for host in self._hosts:
        host.stop()

      # Cancel all stop requests and just to be sure also terminate any associated instances.
      if flags.terminate_hosts:
        requests = self.connection().get_all_spot_instance_requests(filters={'tag:serverset': flags.serverset})
        for req in requests:
          req.cancel()
        instance_ids = [r.instance_id for r in requests if r.instance_id]
        if len(instance_ids) > 0:
          for reservation in self.connection().get_all_instances(instance_ids):
            for instance in reservation.instances:
              instance.terminate()

    self._thread = threading.Thread(name='job', target=run)
    self._thread.start()


  def join(self):
    self._thread.join()


  def get_and_reserve_free_host(self):
    return self._free_hosts.take()


  def unreserve_host(self, host):
    self._free_hosts.put(host)


  def execute_on_next_free_host(self, task):
    host = self.get_and_reserve_free_host()
    host.execute(task)
    return task


  def init_host(self, host):
    """
    Override to initialize host before accepting work. Does nothing by default.
    This runs on the hosts controller thread.
    """
    pass


  def run(self):
    """
    Override this to run the actual job. Default does nothing.
    """
    pass


class Host(object):
  def __init__(self, job, connection, spot_instance_request):
    self._status = ''
    self._task = None
    self._stopped = False
    self._queue = BlockingQueue()
    self._job = job
    self._connection = connection
    self._spot_instance_request = spot_instance_request
    self._instance = None
    self._ssh_client = None
    self._sftp_client = None
    self._cwd = None
    self._log_file_name = os.path.join(flags.logs, self.instance_id() + '.log')
    if not os.path.isdir(flags.logs):
      os.mkdir(flags.logs)
    self._log_file = open(self._log_file_name, 'w+')


  def execute(self, task):
    """
    Execute on the controller thread for this host. Returns immediately, the task will eventually execute.
    """
    varz.tasks_enqueued_count.increment()
    varz.task_queue.object.append(task)
    task.future.on_completion(lambda(future): varz.task_queue.object.remove(task))
    task.future.on_completion(lambda(future): varz.tasks_completed_count.increment())
    task.future.on_completion(lambda(future): self.unreserve())
    self._queue.put(task)


  def refresh_spot_instance_request(self):
    self._spot_instance_request = self._connection.get_all_spot_instance_requests(
      [self._spot_instance_request.id])[0]


  @property
  def status(self):
    return self._status


  def start(self):
    def run():
      self._status = 'waiting_for_instance'
      self.log('Waiting for an instance to get allocated and start up.')
      # Wait until we have an instance allocated.
      while not self._instance:
        time.sleep(10)
        self.refresh_spot_instance_request()
        if self._spot_instance_request.instance_id:
          try:
            self._instance = self._connection.get_all_instances(
              [self._spot_instance_request.instance_id])[0].instances[0]
          except StandardError:
            self.log('Error while loading instance info.')
            pass

      self._status = 'waiting_for_started'
      # Wait until we're ready.
      while not self._instance.state == 'running':
        time.sleep(10)
        self._instance.update()

      # Wait for start up.
      try:
        self.wait_for_connection()
      except StandardError:
        try:
          self.log('Failed to connect. Trying to reboot.')
          self.reboot()
        except StandardError:
          self.log('Could not connect after reboot. Terminating instance.')
          self._instance.terminate()
          # TODO Make new spot request to replace the failed one.

      self._status = 'initializing'
      # Initialize.
      if not flags.skip_init_hosts:
        self._job.init_host(self)

      self.log('Instance is running. Now accepting work.')
      # Start accepting work.
      self.unreserve()
      while not self._stopped:
        self._status = 'waiting_for_work'
        self._task = self._queue.take()
        self._status = 'working'
        self._task.execute(self._job, self)
        self._instance.update()
        if self._instance.state != 'running':
          self.log('Instance stopped. Did the market overbid our spot price?')
          self._stopped = True

      self._status = 'stopped'

      if self._ssh_client:
        self._ssh_client.close()

      # We're done, shut down the instance (if requested).
      if flags.terminate_hosts:
        self._status = 'terminating'
        self.log('Terminating...')
        self._instance.terminate()
        self._status = 'terminated'

    self._thread = threading.Thread(name='host', target=run)
    self._thread.start()


  def __repr__(self):
    spot_id = self._spot_instance_request.id
    instance_id = self._spot_instance_request.instance_id
    return "<%s %s %s status=%s task=%s>" % (
    self.__class__.__name__, spot_id, instance_id, self._status, repr(self._task))


  def stop(self):
    if self._stopped:
      return

    def do_stop(self):
      self.log('Stopping...')
      self._stopped = True

    self.execute(Task(do_stop))
    self._thread.join()


  def reboot(self):
    self._instance.reboot()
    self.wait_for_connection()


  def wait_for_connection(self):
    for tries in reversed(range(10)):
      try:
        self.ping()
        break
      except StandardError:
        if tries == 0:
          raise
      time.sleep(10)


  def check_command_status(self, cmd, status):
    if status:
      self.log('Command failed, more info in %s:\n%s' % (self._log_file_name, cmd))
      raise StandardError('Command failed: ' + cmd)


  def run(self, cmd):
    assert threading.current_thread() == self._thread, """Can only run on the hosts controller thread.
    Use Job#execute_on_next_free_host or Job#get_and_reserve_free_host and Host#execute to schedule work on the hosts controller thread."""
    if self._cwd:
      cmd = 'cd %s && %s' % (self._cwd, cmd)
    self.log('-> %s' % cmd)
    with closing(self.new_channel()) as channel:
      channel.exec_command(cmd)
      status = channel.recv_exit_status()
      self.check_command_status(cmd, status)


  def sudo(self, cmd):
    self.run('sudo %s' % cmd)


  def install_packages(self, packages):
    self.sudo('apt-get -y install %s' % ' '.join(packages))


  def put(self, localpath, remotepath, excludes=[]):
    self.log('Transferring %s to %s' % (localpath, remotepath))
    sftp = self.get_sftp_client()
    sftp.put(localpath, remotepath, confirm=False)
    sftp.chmod(remotepath, os.stat(localpath).st_mode)


  def put_dir(self, localpath, remotepath, excludes=[]):
    """
    Tar a directory recursively to an untar pipe over ssh. Paramiko doesn't seem to handle pipelining very well.
    """
    self.log('Transferring directory %s to %s' % (localpath, remotepath))
    ssh = self.get_ssh_client()
    self.mkdirs(remotepath)
    stdin, stdout, stderr = ssh.exec_command('cd %s && tar vxf -' % remotepath)
    try:
      self.pump_stream(stderr)
      z = tarfile.TarFile(fileobj=stdin, mode='w')

      def tar_recursively(path, name):
        self.log('%s -> %s' % (path, name))
        if os.path.isfile(path):
          z.add(path, arcname=name)
        if os.path.isdir(path):
          for e in os.listdir(path):
            if not e in excludes:
              tar_recursively(os.path.join(path, e), name + '/' + e)

      tar_recursively(localpath, '')
    finally:
      stdin.close()
      stdout.close()
      stderr.close()


  def mkdirs(self, remotepath):
    self.run('mkdir -p %s' % remotepath)


  def get_ssh_client(self):
    if not self._ssh_client:
      for tries in reversed(range(5)):
        try:
          self._ssh_client = paramiko.SSHClient()
          self._ssh_client.set_missing_host_key_policy(paramiko.WarningPolicy())
          self._ssh_client.connect(self._instance.dns_name, username='ubuntu', key_filename=flags.privatekey)
          break
        except StandardError:
          # Retrying.
          self._sftp_client = None
          self._ssh_client = None
          if tries == 0:
            raise
    return self._ssh_client


  def get_sftp_client(self):
    if not self._sftp_client:
      for tries in reversed(range(5)):
        try:
          self._sftp_client = self.get_ssh_client().get_transport().open_sftp_client()
        except StandardError:
          # Retrying.
          self._sftp_client = None
          self._ssh_client = None
          if tries == 0:
            raise

    return self._sftp_client


  def new_channel(self):
    for tries in reversed(range(5)):
      try:
        channel = self.get_ssh_client().get_transport().open_session()
        channel.set_combine_stderr(True)
        self.pump_stream(channel.makefile())
        return channel
      except StandardError:
        # Retrying.
        self._sftp_client = None
        self._ssh_client = None
        if tries == 0:
          raise


  def pump_stream(self, f):
    def pump():
      for line in f:
        self._log_file.write(line)

    threading.Thread(target=pump).start()


  def log(self, msg):
    """
    Log a message and if called from an except class will also print the stack trace.
    """
    sys.stdout.write('[%s] %s\n' % (self.instance_id(), msg))
    self._log_file.write(msg)
    self._log_file.flush()
    if sys.exc_info()[0]:
      traceback.print_exc(sys.stderr)
      traceback.print_exc(self._log_file)


  def instance_id(self):
    if not self._instance:
      return str(self._spot_instance_request.id)
    else:
      return str(self._instance.id)


  def ping(self):
    self.run('echo hello')


  @contextmanager
  def cd(self, dir):
    prev = self._cwd
    self._cwd = dir
    yield
    self._cwd = prev


  def rsync(self, localdir, remotedir, excludes=[], safe_links=False):
    self.mkdirs(remotedir)
    if safe_links:
      safe_links = '--safe-links'
    else:
      safe_links = ''
    cmd = 'rsync -avz -e "ssh -i %(privatekey)s -o \\"StrictHostKeyChecking no\\"" %(excludes)s %(safe_links)s %(localdir)s %(username)s@%(host)s:%(remotedir)s ' % {
      'localdir': localdir, 'remotedir': remotedir, 'privatekey': flags.privatekey, 'username': 'ubuntu',
      'host': self._instance.dns_name, 'excludes': ' '.join(['--exclude=%s' % e for e in excludes]),
      'safe_links': safe_links}
    self.log('(local)-> %s' % cmd)
    p = subprocess.Popen(cmd, shell=True)
    # TODO Log stdout/err of this to log file.
    status = p.wait()
    self.check_command_status(cmd, status)

  def unreserve(self):
    if not self._stopped:
      self._job.unreserve_host(self)

  def join(self):
    self._thread.join()
