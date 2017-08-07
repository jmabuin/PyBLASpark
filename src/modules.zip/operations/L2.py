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

from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.linalg.distributed import IndexedRowMatrix


class L2:

    @staticmethod
    def DGEMV(alpha, A, x, beta, y, jsc):

        # First form y:= beta * y.
        if (beta != 1.0):
            if (beta == 0.0):
                y = Vectors.zeros(y.size)

        else:
            y = beta * y

        if (alpha == 0.0):
            return y


        broadcastVector = jsc.broadcast(x)
        broadcastAlpha = jsc.broadcast(alpha)

        result = A.rows.map(lambda currentRow: L2.MultiplyRows(currentRow.index,
                                                                 broadcastAlpha.value,
                                                                 currentRow.vector,
                                                                 broadcastVector.value))\
            .sortByKey()\
            .values()\
            .collect()

        resultVector = DenseVector(result)

        y = y + resultVector

        return y

    @staticmethod
    def MultiplyRows(index, alpha, row, vector):
        result = alpha * row.dot(vector)

        return (index, result)
