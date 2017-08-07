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
import numpy as np
import math

sys.path.insert(0, '..')

from pyspark import SparkContext
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import IndexedRow
from pyspark.mllib.linalg.distributed import IndexedRowMatrix
from io_spark.IO_Spark import *
from operations.L2 import *


class ConjugateGradient:
    def __init__(self, args, sc):

        self.EPSILON = 1.0e-5

        self.ctx = sc

        self.numPartitions = args.partitions

        self.numIterations = args.iterations
        self.inputVectorPath = args.inputVector
        self.inputMatrixPath = args.inputMatrix
        self.outputVectorPath = args.outputVector

        # Read Matrix input data
        # inputMatrixData = readMatrixRowPerLine(self.inputMatrixPath, self.ctx)

        if (self.numPartitions != 0):
            inputMatrixData = readMatrixRowPerLine(self.inputMatrixPath, self.ctx)\
                .map(lambda line: IndexedRow(line[0], line[1]))\
                .repartition(self.numPartitions)
        else:
            inputMatrixData = readMatrixRowPerLine(self.inputMatrixPath, self.ctx)\
                .map(lambda line: IndexedRow(line[0], line[1]))

        self.inputMatrix = IndexedRowMatrix(inputMatrixData)

        self.inputVector = readVector(self.inputVectorPath, self.ctx)

        if (self.numIterations == 0):
            self.numIterations = self.inputVector.size * 2

        self.result = Vectors.zeros(self.inputVector.size)

    def solve(self):
        # print result

        stop = False

        start = time.clock()

        r = np.copy(self.inputVector)

        Ap = Vectors.zeros(self.inputMatrix.numRows())

        # p = r
        p = np.copy(r)

        # rsold = r * r

        rsold = r.dot(r)

        rsold = r.dot(r)

        alpha = 0.0

        rsnew = 0.0

        k = 0

        while (not stop):

            # Inicio -- Ap=A * p
            Ap = L2.DGEMV(1.0, self.inputMatrix, p, 0.0, Ap, self.ctx)

            # Fin -- Ap=A * p

            # alpha=rsold / (p'*Ap)
            alpha = rsold / p.dot(Ap);

            # x=x+alpha * p
            self.result = self.result + alpha*p

            # r=r-alpha * Ap
            r = r - alpha*Ap

            # rsnew = r'*r
            rsnew = r.dot(r)

            if ((math.sqrt(rsnew) <= self.EPSILON) or (k >= (self.numIterations))):
                stop = True

            # p=r+rsnew / rsold * p
            p = r + (rsnew/rsold) * p

            rsold = rsnew

            k += 1

        # FIN GRADIENTE CONJUGADO

        end = time.clock()

        print "Total time in solve system is: " + str(end - start) + " and " + str(k) + " iterations."

        printVector(self.result)

        return self.result