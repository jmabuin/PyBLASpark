from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.linalg.distributed import IndexedRow


def readMatrixRowPerLine(matrixPath, ctx):

    data = ctx.textFile(matrixPath).filter(lambda line: ((not line.startswith("%")) and (":" in line)))\
        .map(lambda matrixLine: MatrixLine2Pair(matrixLine))

    #print "Number of rows: " + str(data.count())

    return data


def MatrixLine2Pair(line):
    newPair = line.split(":")

    key = int(newPair[0])

    valuesStr = newPair[1]
    valuesArray = valuesStr.split(",")

    values = []

    for value in valuesArray:
        if value:
            values.append(float(value.strip()))

    return (key, values)


def readVector(vectorPath, ctx):
    data = ctx.textFile(vectorPath).zipWithIndex()\
        .map(lambda item : (item[1], item[0]))\
        .sortByKey()\
        .collect()

    values = []

    currentItem = 0

    for item in data:

        if(currentItem >= 2):
            values.append(float(item[1]))

        currentItem += 1

    returnItem = DenseVector(values)

    return returnItem


