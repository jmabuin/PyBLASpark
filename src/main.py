import os
import sys
import time
import importlib

from pyspark import SparkContext, SparkConf, SQLContext

sys.path.insert(0, 'modules.zip')

from options.Options import *
from options.Mode import *
from jobs.Jobs import *

if __name__ == '__main__':
    initTime = time.clock()

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    #options_module = importlib.import_module('options')
    #jobs_module = importlib.import_module('jobs')

    myOptions = Options() #options._module.Options()

    if myOptions.mode == Mode.DMXV:
        job = Jobs(myOptions, sc)

    endTime = time.clock()

    print "Total time used: " + str(endTime - initTime) + "\n"
