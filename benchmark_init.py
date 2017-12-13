from cProfile import Profile
import six
import argparse
import logging

import signac
from tqdm import tqdm

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory

parser = argparse.ArgumentParser()
parser.add_argument('N', type=int, default=100)
parser.add_argument('--debug', action='store_true')
args = parser.parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


with TemporaryDirectory() as tmpdir:
    project = signac.init_project(root=tmpdir, name='benchmark-init')
    profile = Profile()
    profile.enable()
    for i in tqdm(range(args.N)):
        project.open_job({'a': i}).init()
    profile.disable()
    profile.print_stats(sort='ncalls')
