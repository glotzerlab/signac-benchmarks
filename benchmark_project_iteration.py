# Copyright 2017 The Regents of the University of Michigan
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import argparse
import logging
from time import time
from itertools import islice

from tqdm import tqdm

from benchmark_signac import setup_random_project

parser = argparse.ArgumentParser()
parser.add_argument('N', type=int, default=100)
parser.add_argument('--debug', action='store_true')
args = parser.parse_args()


if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


with setup_random_project(args.N) as project:
    project.update_cache()
    start = time()
    for doc in tqdm(islice(project, 1000), total=min(len(project), 1000)):
        pass
    stop = time()
    print("Total time (N={}): {:.2f}s".format(args.N, stop - start))
    print("Time / N: {:.3f}\u00B5s".format(1e6 * (stop - start) / args.N))
