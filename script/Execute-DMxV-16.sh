#!/usr/bin/env bash

spark-submit --py-files modules.zip --master yarn --deploy-mode cluster --num-executors 4 main.py -d --alpha 1.0 --beta 0.0 Matrices/Matriz16-Sym.mtx Matrices/Vector-16.mtx Matrices/SaidaProbaPython-Vector-16.mtx