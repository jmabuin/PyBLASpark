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

import argparse
from Mode import *


class Options:

    def __init__(self):
        parser = argparse.ArgumentParser()

        parser.add_argument("-d", "--dmxv", action="store_true", default=False,
                            help="Performs a distributed dense matrix dot vector operation")

        parser.add_argument("-s", "--smxv", action="store_true", default=False,
                            help="Performs a distributed sparse matrix dot vector operation")

        parser.add_argument("-c", "--conjGrad", action="store_true", default=False,
                            help="Solves a system by using the conjugate gradient method")

        parser.add_argument("-i", "--iterations", type=int, default=0,
                            help="Number of iterations to perform the conjugate gradient method")

        parser.add_argument("-l", "--pairLine", action="store_true", default=True,
                            help="The matrix format will be a IndexedRowMatrix")

        parser.add_argument("-o", "--coordinate", action="store_true", default=False,
                            help="The matrix format will be a CoordinateMatrix")

        parser.add_argument("-b", "--blocked", action="store_true", default=False,
                            help="The matrix format will be a BlockMatrix")

        parser.add_argument("-p", "--partitions", type=int, default=0,
                            help="Number of partitions to divide the matrix")

        parser.add_argument("--rows", type=int, default=0,
                            help="Number of rows for block in BlockMatrix format")

        parser.add_argument("--cols", type=int, default=0,
                            help="Number of vols for block in BlockMatrix format")

        parser.add_argument("--alpha", type=float, default=0.0,
                            help="Alpha value for DMxV example")

        parser.add_argument("--beta", type=float, default=0.0,
                            help="Beta value for DMxV example")

        parser.add_argument("inputMatrix", type=str, default="",
                            help="Matrix file name")

        parser.add_argument("inputVector", type=str, default="",
                            help="Input Vector file name")

        parser.add_argument("outputVector", type=str, default="",
                            help="Output Vector file name")

        self.args = parser.parse_args()

        if(self.args.dmxv):
            self.mode = Mode.DMXV
        elif(self.args.smxv):
            self.mode = Mode.SMXV
        elif(self.args.conjGrad):
            self.mode = Mode.CG
        else:
            self.mode = Mode.HELP

        if (self.mode == Mode.HELP):
            parser.print_help()
            quit(code=1)
