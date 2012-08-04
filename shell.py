from __init__ import flags
import store
import sys

if __name__ == '__main__':
  fls = flags.non_flag_components()
  if len(fls) != 1:
    print 'expected one parameter.'
    sys.exit(1)
  s = store.Store(fls[0])
