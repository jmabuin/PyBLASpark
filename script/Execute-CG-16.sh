#!/usr/bin/env bash

spark-submit --py-files modules.zip --master yarn --deploy-mode cluster --num-executors 4 main.py -c Matrices/Matriz16-Sym.mtx Matrices/Vector-16.mtx Matrices/SaidaProbaPythonCG-Vector-16.mtx