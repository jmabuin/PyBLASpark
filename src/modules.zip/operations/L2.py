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
