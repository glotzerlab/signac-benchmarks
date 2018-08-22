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
import json
import argparse

import pandas as pd
from signac import Collection

from complexity import COMPLEXITY


def strip_complexity(cat):
    if len(cat) > 1 and cat[1] == '_':
        return COMPLEXITY[cat[2:]], cat[2:]
    else:
        return COMPLEXITY.get(cat), cat


def normalize(data, N):
    for cat, x in data.items():
        cplx, cat_ = strip_complexity(cat)
        x_mean = min([(y/n) for n, y in x])
        if cplx is not None:
            x_mean /= eval(cplx)
        yield cat, 1e3 * x_mean


def tr(s):
    cplx, cat = strip_complexity(s)
    t = {
        'select_by_id': "Select by ID",
        'determine_len': "Determine N",
        'iterate': "Iterate (multiple passes)",
        'iterate_single_pass': "Iterate (single pass)",
        'search_lean_filter': "Search w/ lean filter",
        'search_rich_filter': "Search w/ rich filter",
        'datreant.core': "datreant",
        'tool,N': "Tool, N",
    }.get(cat, cat)
    if cplx is not None:
        t += ' O({})'.format(cplx)
    return t


def read_benchmark(filename, filter):
    with Collection.open(filename) as c:
        docs = list(c.find(filter))

    df_meta = pd.DataFrame(
        {doc['_id']: doc['meta'] for doc in docs}).T
    df_data = pd.DataFrame(
        {doc['_id']: dict(normalize(doc['data'], doc['meta']['N'])) for doc in docs}).T

    return pd.concat([df_meta, df_data], axis=1)


def main(args):
    filter = json.loads(args.filter) if args.filter else None
    df = read_benchmark(args.filename, filter)
    print("All values in ms.")
    print(df.rename(columns=tr).groupby(['tool', 'N']).mean().round(2).T)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'filename', default='benchmark.txt', nargs='?',
        help="The collection that contains the benchmark data.")
    parser.add_argument(
        '-f', '--filter', type=str,
        help="Select a subset of the data.")
    args = parser.parse_args()

    main(args)
