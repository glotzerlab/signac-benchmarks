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
