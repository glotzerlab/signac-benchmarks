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
