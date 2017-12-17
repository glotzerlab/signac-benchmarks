import signac
import argparse
import logging
from benchmark_signac import generate_random_data

parser = argparse.ArgumentParser()
parser.add_argument('N', type=int, help="The project size.")
parser.add_argument('--debug', action='store_true')
parser.add_argument('--prefix', default='projects/')
args = parser.parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

name = 'TestCachingProject-{}'.format(args.N)
root = os.path.join(args.prefix, 'test-project-{}'.format(args.N))

try:
    project = signac.get_project(root=root)
    if len(project) != args.N:
        raise RuntimeError("Bad project.")
except LookupError:
    project = signac.init_project(name, root=prefix)
    generate_random_data(project, args.N, 10, 10, 100, 0)
    project.update_cache()
