import signac
import argparse
import logging
from itertools import islice
from time import time

from tqdm import tqdm

import benchmark as b


parser = argparse.ArgumentParser()
parser.add_argument('N', type=int, default=100)
parser.add_argument('--debug', action='store_true')
args = parser.parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

with b.setup_random_project(args.N) as p:
    print("Without cache:")
    project = signac.get_project(root=p.root_directory())
    start = time()
    for doc in tqdm(islice(project, 1000), total=min(len(project), 1000)):
        pass
    stop = time()
    print("Total time (N={}): {:.2f}s".format(args.N, stop - start))
    print("Time / N: {:.3f}\u00B5s".format(1e6 * (stop - start) / args.N))

    print("With cache:")
    project = signac.get_project(root=p.root_directory())
    project.update_cache()
    start = time()
    for doc in tqdm(islice(project, 1000), total=min(len(project), 1000)):
        pass
    stop = time()
    print("Total time (N={}): {:.2f}s".format(args.N, stop - start))
    print("Time / N: {:.3f}\u00B5s".format(1e6 * (stop - start) / args.N))
