#!/bin/bash

spark-submit --py-files jobs.zip --master yarn --deploy-mode cluster --num-executors 4 main.py -d --alpha 1.0 --beta 0.0 Matrices/Matriz-8192-RPL-Symm.mtx Matrices/Vector-8192.mtx Matrices/SaidaProbaPython-Vector-8192.mtx