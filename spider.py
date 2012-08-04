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
      return None, httperror.code
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

  def Queue(self, url):
    now = time.time()
    for i in range(self._size):
      if self._status[i] == CrawlQueue.EMPTY or now - self._started[i] > 120:
        if self._status[i] != CrawlQueue.EMPTY:
          print 'THREAD NEVER RETURNED!', now, self._started[i]
        self._status[i] = CrawlQueue.CRAWLING
        self._started[i] = now
        self._results[i] = (url, url, '')
        thread.start_new_thread(self.Crawl, (i, url))
        #print '>>', i, url
        return True
    return False

  def QueueStatus(self):
    print self._status

  def Pop(self):
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
        #if self._status[i] == CrawlQueue.SUCCESS:
        #  print '<<', i, res[1][0]
        return res
    return None

  def Crawl(self, i, url):
    try:
      self._results[i] = (url, None, None)
      final_url, html = GetUrl(url)
      self._results[i] = (url, final_url, html)
      self._status[i] = CrawlQueue.SUCCESS
    
    except httplib.InvalidURL:
      print 'Invalid url: ', url
      self._results[i] = (url, '', '')
      self._status[i] = CrawlQueue.SUCCESS
      
    except ValueError:
      print 'Invalid url: ', url
      self._results[i] = (url, '', '')
      self._status[i] = CrawlQueue.SUCCESS
      
    except Exception, e:
      print 'error:', url, e
      self._status[i] = CrawlQueue.FAILURE

  def Empty(self):
    for i in range(self._size):
      if self._status[i] != CrawlQueue.EMPTY:
        return False
    return True

  def HasRoom(self):
    for i in range(self._size):
      if self._status[i] == CrawlQueue.EMPTY:
        return True
    return False


class Spider(object):
  def __init__(self, starturl, sleepinbetween=0.0, num_threads=3):
    self._sleepinbetween = sleepinbetween
    self._num_threads = num_threads
    self._todo = [starturl]
    self._done = set()

  def queue(self, url):
    if not url in self._done:
      self._todo.append(url)

  def crawl(self):
    """Yield tupels (url, final_url, html) as long as there is work to be done."""
    cq = CrawlQueue(threads)
    count = 0
    while self._todo or not cq.Empty():
      if self._todo and cq.HasRoom():
        url_part = self._todo.pop()
        self._done.add(url_part)
        #print url_part
        orgurl = self.MakeUrl(url_part)
        time.sleep(self.SleepBetween())
        cq.Queue(orgurl)

      new_item = cq.Pop()
      if new_item:
        state, res = new_item
        if state == CrawlQueue.FAILURE:
          print 'Connection died. Try later.'
          sys.exit(0)
        org_url, final_url, html = res
        if state == CrawlQueue.TIME_OUT:
          cq.Queue(org_url)
          continue
        
        yield org_url, final_url, html
