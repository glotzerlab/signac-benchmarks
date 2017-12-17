import json
import argparse
from math import log, sqrt

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


def main(args):
    with Collection.open(args.filename) as c:
        if args.filter:
            docs = list(c.find(json.loads(args.filter)))
        else:
            docs = list(c)

    df_meta = pd.DataFrame(
        {doc['_id']: doc['meta'] for doc in docs}).T
    df_data = pd.DataFrame(
        {doc['_id']: dict(normalize(doc['data'], doc['meta']['N'])) for doc in docs}).T

    df = pd.concat([df_meta, df_data], axis=1)

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
