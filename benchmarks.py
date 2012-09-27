#!/usr/bin/env python

__author__ = 'douweosinga'

import json
import time
import cPickle

OBJECT_TO_SAFE = {'comes_from': 18082382,
                  u'coo_sources': {u'facebook': (40.21088, -8.42709),
                                   u'touristeye': (40.21051, -8.42856)},
                  u'description': u'',
                  u'descriptions': {u'facebook': u'', u'touristeye': u''},
                  u'facebook_checkins': 29,
                  u'facebook_id': u'205985002757321',
                  u'facebook_likes': 714,
                  u'facebook_talking_about': 8,
                  u'id': u'%C3%81-Capella-Coimbra-p-509044',
                  u'images': [(u'touristeye',
                               u'http://tephotos.s3.amazonaws.com/places/web/A-Capella_101fa46e145d038c8e29ba8bc1d891d40d2ff4c1.jpg')],
                  u'lat': 40.21088,
                  u'lng': -8.42709,
                  u'loc_sources': [(u'touristeye',
                                    u'http://www.touristeye.com/Coimbra-p-1976')],
                  u'match_scores': [(u'touristeye', {u'reason': u'original seed'}),
                                    (u'facebook',
                                     {u'bonus': u'*0.8 for poicat missing',
                                      u'candidates': 0,
                                      u'distance': 0.1311445215713754,
                                      u'id': u'%C3%81-Capella-Coimbra-p-509044',
                                      u'language': u'',
                                      u'lat': 40.21051,
                                      u'lng': -8.42856,
                                      u'name': u'\xc1 Capella',
                                      u'org_name': u'\xe0Capella',
                                      u'score': 0.2,
                                      u'text_score': 0.25})],
                  u'name': u'\xe0Capella',
                  u'names': {u'facebook': u'\xe0Capella', u'touristeye': u'\xc1 Capella'},
                  u'normalized_hours': None,
                  u'poicat': u'see',
                  'poitype': ('sight', 0.01, 0.9),
                  u'properties': {u'address': u'Rua Corpo de Deus',
                                  u'hours': u'',
                                  u'phone': u'+351239833985 / +351918113307'},
                  u'sources': [(u'touristeye',
                                u'http://www.touristeye.com/%C3%81-Capella-Coimbra-p-509044'),
                               (u'facebook',
                                u'http://www.facebook.com/pages/%C3%A0Capella/205985002757321')],
                  }

def test_json():
  x = json.dumps(OBJECT_TO_SAFE)
  y = json.loads(x)

def test_pickl():
  x = cPickle.dumps(OBJECT_TO_SAFE)
  y = cPickle.loads(x)

def benchmark(method, count):
  start_time = time.time()
  for i in xrange(count):
    method()
  return (time.time() - start_time) / count

if __name__ == '__main__':
  print 'json:', benchmark(test_json, 1000)
  print 'pickl:', benchmark(test_pickl, 1000)



