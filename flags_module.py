#!/usr/bin/python

# Flags module
import sys
import os

import base

class FlagError(Exception):
  pass

# cache current directory so it's stable
curdir = os.getcwd()
state_fp = None

def fileparser(value):
  """Make value a filename independent of os."""
  if not value:
    return ''
  value = value.replace('\\', '/')
  relative = not value[0] == '/'
  bits = value.split('/')
  res = apply(os.path.join, bits)
  if not relative:
    res = os.path.sep + res
  return res

class FlagType(object):
  pass

class FlagTypeExtern(FlagType):
  """A FlagType to indicate that this value is declared elsewhere."""
  pass


class Flags(object):
  def __init__(self):
    self._descriptions = {}
    self._vals = {}
    self._non_flags = []
    for arg in sys.argv[1:]:
      if not arg.startswith('--'):
        self._non_flags.append(arg)
        continue
      arg = arg[2:]
      if '=' in arg:
        key, val = arg.split('=', 1)
      elif arg.startswith('no'):
        key = arg[2:]
        val = False
      else:
        key = arg
        val = True
      key = key.lower()
      self._vals[key] = val
    self._unaccounted_for = set(self._vals.keys())
    self._init_done = True

  def __setattr__(self, name, flag_class):
    if hasattr(self, '_init_done'):
      if not issubclass(flag_class, FlagType):
        raise FlagError('%s is not a flag type' % flag_class)
      if flag_class == FlagTypeExtern:
        if not hasattr(self, name):
          raise FlagError('%s is declared external, but cannot be found elsewhere')
        return
      arg_value = self._vals.get(name)
      flag_value = flag_class(arg_value)
      self._descriptions[name] = flag_value.description()
      if flag_value.value() is None:
        raise FlagError('%s is a required flag' % name)
      if name in self._unaccounted_for:
        self._unaccounted_for.remove(name)
      object.__setattr__(self, name, flag_value.value())
    else:
      # happens during __init__
      value = flag_class
      object.__setattr__(self, name, value)

  def non_flag_components(self):
    """Call to get to the non flags, execute help, check for flags non existence."""
    if self._vals.get('help'):
      print 'options:'
      keys = self._descriptions.keys()
      keys.sort()
      for key in keys:
        print '  ', key, self._descriptions[key]
      sys.exit(0)
    if self._unaccounted_for:
      for flag in self._unaccounted_for:
        print '%s is not an expected flag' % flag
      sys.exit(1)
    return self._non_flags

  @classmethod
  def flag_type(cls, operator, defaultvalue, descriptionvalue):
    class FlagClass(FlagType):
      def __init__(self, value):
        self._description = descriptionvalue
        if value is None:
          self._value = defaultvalue
        else:
          self._value = operator(value)

      def value(self):
        return self._value

      def description(self):
        return self._description

    return FlagClass

  @classmethod
  def StateFp(cls):
    """Returns a fingerprint of the state defined by the paramers as a string."""
    global state_fp
    global curdir
    state = str(sys.argv) + curdir
    if state_fp is None:
      state_fp = '%08x' % base.fingerprint(state)
    return state_fp

  def String(self, default_value='', description=''):
    return Flags.flag_type(str, default_value, description)

  def Int(self, default_value=0, description=''):
    return Flags.flag_type(int, default_value, description)

  def Bool(self, default_value=False, description=''):
    return Flags.flag_type(bool, default_value, description)

  def Float(self, default_value=0.0, description=''):
    return Flags.flag_type(float, default_value, description)

  def File(self, default_value=None, description=''):
    return Flags.flag_type(fileparser, default_value, description)

  def Extern(self):
    return FlagTypeExtern


flags = Flags()
