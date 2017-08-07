from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.linalg.distributed import IndexedRow

import subprocess
import os

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


def writeMatrixRowPerLine(matrixPath, matrix):

    numRows = matrix.numRows()
    numCols = matrix.numCols()

    tmpFileName = os.path.basename(matrixPath)

    localRows = matrix.rows.collect()
    sortedRows = []

    newFile = open(tmpFileName, 'w')

    newFile.write("%%MatrixMarket matrix array real general")
    newFile.write(str(numRows)+" "+str(numCols)+" "+str(numRows * numCols))

    for currentRow in localRows:

        sortedRows.insert(currentRow.index, currentRow)

    for currentRow in sortedRows:
        newDataRow = str(currentRow.index) + ":"

        for item in newDataRow.value:
            newDataRow = newDataRow + str(item) + ","

        newFile.write(newDataRow)

    newFile.close()

    returnValue = subprocess.call(["hdfs", "dfs", "-put", tmpFileName, matrixPath])

    os.remove(tmpFileName)


def writeVector(vectorPath, vector):

    numItems = vector.size

    tmpFileName = os.path.basename(vectorPath)

    print "TMP filename is " + tmpFileName + " from total " + vectorPath

    newFile = open(tmpFileName, 'w')

    newFile.write("%%MatrixMarket matrix array real general")
    newFile.write(str(numItems)+" 1")

    for currentItem in vector:

        newFile.write(str(currentItem))

    newFile.close()

    dest = os.path.dirname(vectorPath)

    if not dest:
        returnValue = subprocess.call(["hdfs", "dfs", "-put", tmpFileName])
    else:
        returnValue = subprocess.call(["hdfs", "dfs", "-put", tmpFileName, os.path.dirname(vectorPath)+"/"])

    print "returned value from hdfs put is: " + str(returnValue)

    os.remove(tmpFileName)


def printVector(vector):
    numItems = vector.size

    print "%%MatrixMarket matrix array real general"
    print str(numItems) + " 1"

    for currentItem in vector:
        print str(currentItem)
