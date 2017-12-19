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
