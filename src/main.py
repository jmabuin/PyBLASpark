#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
# Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
#
# This file is part of PyBLASpark.
#
# PyBLASpark is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PyBLASpark is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with PyBLASpark. If not, see <http://www.gnu.org/licenses/>.
"""

# __author__ = "José M. Abuín"
# __credits__ = ["José M. Abuín"]
# __license__ = "GPLv3"
# __version__ = "0.0.1"
# __maintainer__ = "José M. Abuín"
# __email__ = "josemanuel.abuin@usc.es"
# __status__ = "Development"

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

    job = Jobs(myOptions, sc)

    endTime = time.clock()

    print "Total time used: " + str(endTime - initTime) + "\n"
