import os
import sys
import inspect
#currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
#parentdir = os.path.dirname(currentdir)
#sys.path.insert(0, parentdir)

sys.path.insert(0, '..')

from DMxV import *
from options.Mode import *


class Jobs:

    def __init__(self, args, sc):

        if(args.mode == Mode.DMXV):
            #sc.getConf().setAppName("PyBLASpark - Example DmXV")
            exDMxV = DMxV(args.args, sc)