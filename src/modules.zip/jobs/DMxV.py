import os
import sys
import inspect

#currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
#parentdir = os.path.dirname(currentdir)
sys.path.insert(0, '..')

from pyspark import SparkContext
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import IndexedRow
from pyspark.mllib.linalg.distributed import IndexedRowMatrix
from io_spark.IO_Spark import *
from operations.L2 import *

#from io_spark import *
#from operations import *


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

