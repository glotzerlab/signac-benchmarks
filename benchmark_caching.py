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
import os
import sys
from time import time
import logging

import signac
from tqdm import tqdm

from benchmark import generate_random_data, fmt_size
from project_benchmark import determine_project_size

logging.basicConfig(level=logging.DEBUG)

N = int(sys.argv[1])

name = 'TestCachingProject-{}'.format(N)
prefix = 'projects/test-caching-{}'.format(N)

try:
    project = signac.get_project(root=prefix)
except LookupError:
    project = signac.init_project(name, root=prefix)
    generate_random_data(project, N, 10, 10, 100, 25)
    project.update_cache()

project._read_cache()

print('JSON metadata size', fmt_size(determine_project_size(project)['statepoint_metadata_size']))
for (label, fn) in [
    ('JSON state points file', 'signac_statepoints.json'),
    ('JSON binary file', '.json-cache.dat'),
    ('JSON compressed file', project.FN_CACHE),
    ('pickled cache', '.cache.dat'),
    ('shelve cache', '.cache.db'),
]:
    try:
        print(label, fmt_size(os.path.getsize(project.fn(fn))))
    except FileNotFoundError:
        pass


# WITH CACHE
project = signac.get_project(root=prefix)
start = time()
for job in tqdm(project, desc='with cache'):
    pass
stop = time()
print("with cache, time / N: {:.3f}\u00B5s".format(1e6 * (stop - start) / len(project)))
