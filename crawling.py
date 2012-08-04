#!/usr/bin/python

import socket
from norwegianblue import flags
from norwegianblue import store

import os
import re
import status
import random
import thread
import httplib
import urllib2
import subprocess
import sys
import time
import urlparse
import urllib
import socket
from third import simplejson

import store

flags.url_fetch_timeout = flags.Int(60000)
flags.use_wget = flags.Bool(False)

STATUS = {}

cookie = None
total_time = 0
requests = 0

def get_url(url, tries=5, minsleep=0.0, IsError=None):
  if flags.use_wget:
    pipe = subprocess.Popen('wget -r %s' % url,
                            shell=True,
                            stdout=subprocess.PIPE).stdout
    html = pipe.read()
    return url, html
  sleep = 2.0
  STATUS[url] = 'init'
  socket.setdefaulttimeout(float(flags.url_fetch_timeout) / 1000)
  for tries in range(5):
    try:
      STATUS[url] = 'sleeping'
      time.sleep(minsleep)
      request = urllib2.Request(url)
      request.add_header('User-Agent', 'Marco Polo')
      if cookie:
        request.add_header('Cookie', cookie)
      opener = urllib2.build_opener()
      STATUS[url] = 'opening'
      handle = opener.open(request)
      STATUS[url] = 'postprocessing'
      final_url = handle.geturl()
      html = handle.read()
      if IsError and IsError(html):
        print 'service in error'
      else:
        STATUS[url] = 'alldone'
        return final_url, html
      STATUS[url] = 'errorsleep'
      time.sleep(sleep)
      sleep *= 3
    except urllib2.HTTPError, httperror:
      return None, httperror
    except urllib2.URLError, urlerror:
      print 'urllib:', urlerror.reason
      if 'no host' in urlerror.reason:
        return url, ''
    except IOError, io:
      print 'ioerror', io, type(io)
      pass
    except socket.error:
      print 'socket error'
      pass
  STATUS[url] = 'inerror'
  return None, None


def google_json_search(q, extra=''):
  if type(q) == unicode:
    q = q.encode('utf-8')
  url = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&q=%s%s' % (urllib.quote_plus(q), extra)
  search_results = urllib.urlopen(url)
  return simplejson.loads(search_results.read())


def get_google_count(q, extra=''):
  count = None
  tries = 3
  while count is None:
    try:
      json = google_json_search(q, extra)
      cursor = json['responseData']['cursor']
      count = cursor.get('estimatedResultCount', 0)
    except TypeError:
      tries -= 1
      if tries == 0:
        raise
  print q, count
  return count


def PrintStatus():
  print STATUS.values()

class CrawlQueue(object):
  EMPTY, CRAWLING, SUCCESS, FAILURE, TIME_OUT = range(5)

  def __init__(self, size):
    self._size = size
    self._results = [None] * size
    self._status = [CrawlQueue.EMPTY] * size
    self._started = [0] * size
    for i in range(self._size):
      status.Set('thread_%i' % i,
                 lambda self=self, i=i: self._status[i])

  def queue(self, url, crawl_id):
    now = time.time()
    for i in range(self._size):
      if self._status[i] == CrawlQueue.EMPTY or now - self._started[i] > 120:
        if self._status[i] != CrawlQueue.EMPTY:
          print 'THREAD NEVER RETURNED!', now, self._started[i]
        self._status[i] = CrawlQueue.CRAWLING
        self._started[i] = now
        self._results[i] = (crawl_id, url, '')
        thread.start_new_thread(self.crawl, (i, url, crawl_id))
        return True
    return False

  def queue_status(self):
    print self._status

  def pop(self):
    now = time.time()
    for i in range(self._size):
      if (self._status[i] == CrawlQueue.SUCCESS or self._status[i] == CrawlQueue.FAILURE 
          or (self._status[i] == CrawlQueue.CRAWLING and now - self._started[i] > 120)):
        if self._status[i] == CrawlQueue.CRAWLING:
          status = CrawlQueue.TIME_OUT
        else:
          status = self._status[i]
        res = status, self._results[i]
        self._status[i] = CrawlQueue.EMPTY
        return res
    return None

  def crawl(self, i, url, crawl_id):
    try:
      self._results[i] = (crawl_id, None, None)
      start_time = time.time()
      final_url, html = get_url(url)
      global total_time
      global requests
      total_time += time.time() - start_time
      requests += 1
      self._results[i] = (crawl_id, final_url, html)
      self._status[i] = CrawlQueue.SUCCESS
    
    except httplib.InvalidURL:
      print 'Invalid url: ', url
      self._results[i] = (crawl_id, '', '')
      self._status[i] = CrawlQueue.SUCCESS
      
    except ValueError:
      print 'Invalid url: ', url
      self._results[i] = (crawl_id, '', '')
      self._status[i] = CrawlQueue.SUCCESS
      
    except Exception, e:
      print 'error:', crawl_id, url, e
      self._status[i] = CrawlQueue.FAILURE

  def is_empty(self):
    for i in range(self._size):
      if self._status[i] != CrawlQueue.EMPTY:
        return False
    return True

  def has_room(self):
    for i in range(self._size):
      if self._status[i] == CrawlQueue.EMPTY:
        return True
    return False
  
  def active_workers(self):
    count = 0
    for i in range(self._size):
      if self._status[i] != CrawlQueue.EMPTY:
        count += 1
    return count


class CrawlJob(object):
  def __init__(self, num_threads=3, sleepinbetween=0.3):
    self._start_time = time.time()
    self._request_count = 0
    self._sleepinbetween = sleepinbetween
    self._num_threads = num_threads
    self._todo = []
    self._done = set()

  def qps(self):
    if self._request_count > 0:
      return float(self._request_count) / (time.time() - self._start_time)

  def queue(self, url, crawl_id=None):
    """Schedule a url for crawling.
    
    crawl_id will be returned when the results are ready and can be used to identify the request.
    If non is specified this defaults to url. Since the CrawlJob takes into account redirects, the
    crawl_id might be different from the final url when the results are yielded by crawl().
    """
    if crawl_id is None:
      crawl_id = url
    if not url in self._done:
      self._todo.append((url, crawl_id))
  
  def set_url_provider(self, provider):
    """Set the provider for the urls to be crawled, rather than setting them one by on in queue.
    
    The provided object should implement __nonzero__ to indicate whether there are more urls
    and should implement the pop method to provide the next (url, crawlid) tupel.
    
    Python lists provide this, so calling job.set_url_provider([(url1, 0), (url2, 1), (url3, 2)])
    will work.
    """
    self._todo = provider

  def crawl(self):
    """Yield tupels (crawl_id, final_url, html) as long as there is work to be done."""
    cq = CrawlQueue(self._num_threads)
    count = 0
    while self._todo or not cq.is_empty():
      if self._todo and cq.has_room():
        url, crawl_id = self._todo.pop()
        self._done.add(url)
        cq.queue(url, crawl_id)
        continue
      new_item = cq.pop()
      if new_item:
        state, res = new_item
        if state == CrawlQueue.FAILURE:
          print 'Connection died. Try later.'
          sys.exit(0)
        crawl_id, final_url, html = res
        if state == CrawlQueue.TIME_OUT:
          cq.queue(final_url, crawl_id)
          continue

        self._request_count += 1
        yield crawl_id, final_url, html
        continue
      if requests % 100 == 1:
        print cq.active_workers(), total_time / requests

      time.sleep(self._sleepinbetween)
