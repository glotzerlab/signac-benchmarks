import json
import argparse

import pandas as pd
from signac import Collection


def strip_complexity(cat):
    assert cat[1] == '_'
    c = cat[0]
    return c, cat[2:]


def normalize(data, N):
    for cat, x in data.items():
        cplx, cat_ = strip_complexity(cat)
        x_mean = min([(y/n) for n, y in x])
        if cplx == 'N':
            x_mean /= N
        if cat_ == 'determine_len':
            x_mean *= 100
        yield cat, 1e3 * x_mean


def tr(s):
    return {
        '1_select_by_id': "Select by ID O(1)",
        'N_determine_len': "Determine N (x100) O(N)",
        'N_iterate': "Iterate (multiple passes) O(N)",
        'N_iterate_single_pass': "Iterate (single pass) O(N)",
        'N_search_lean_filter': "Search w/ lean filter O(N)",
        'N_search_rich_filter': "Search w/ rich filter O(N)",
        'datreant.core': "datreant",
        'tool,N': "Tool, N",
    }.get(s, s)


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
