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
import inspect

sys.path.insert(0, '..')

from pyspark import SparkContext
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import IndexedRow
from pyspark.mllib.linalg.distributed import IndexedRowMatrix
from io_spark.IO_Spark import *
from operations.L2 import *


class DMxV:
    def __init__(self, args, sc):
        self.ctx = sc

        self.numPartitions = args.partitions

        self.inputVectorPath = args.inputVector
        self.inputMatrixPath = args.inputMatrix
        self.outputVectorPath = args.outputVector

        self.alpha = args.alpha
        self.beta = args.beta

        # Read Matrix input data
        # inputMatrixData = readMatrixRowPerLine(self.inputMatrixPath, self.ctx)

        if (self.numPartitions != 0):
            inputMatrixData = readMatrixRowPerLine(self.inputMatrixPath, self.ctx)\
                .map(lambda line: IndexedRow(line[0], line[1]))\
                .repartition(self.numPartitions)
        else:
            inputMatrixData = readMatrixRowPerLine(self.inputMatrixPath, self.ctx)\
                .map(lambda line: IndexedRow(line[0], line[1]))

        print "Number of rows in Matrix with type" + str(type(inputMatrixData)) + " is: " + str(inputMatrixData.count())

        # PipelinedRDD to RDD
        # newData = sc.parallelize(inputMatrixData.collect())

        inputMatrix = IndexedRowMatrix(inputMatrixData)

        inputVector = readVector(self.inputVectorPath, self.ctx)

        print "Vector size is: " + str(inputVector.size)

        result = Vectors.zeros(inputVector.size)

        # print result

        # DGEMV(alpha, A, x, beta, y, jsc):
        result = L2.DGEMV(self.alpha, inputMatrix, inputVector, self.beta, result, self.ctx)

        # writeVector(self.outputVectorPath, result)

        printVector(result)

