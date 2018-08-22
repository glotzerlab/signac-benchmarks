import json
import argparse

from report_benchmark import read_benchmark


result_columns = [
    'determine_len',
    'iterate',
    'iterate_single_pass',
    'search_lean_filter',
    'search_rich_filter',
    'select_by_id',
]


def main(args):
    filter = json.loads(args.filter) if args.filter else None

    df = read_benchmark(args.filename, filter)
    df_cmp = read_benchmark(args.filename_cmp, filter)

    benchmark = df[df.tool == 'signac'][result_columns].min()
    compare = df_cmp[df_cmp.tool == 'signac'][result_columns].min()

    # Calculate scores, where a score larger than 1 means the benchmark
    # is faster than the comparison.
    scores = compare / benchmark
    print(scores)
    print(scores.min())
    if scores.min() < args.pass_above:
        raise RuntimeError(
            "The measured score ({}) is below the required minimal "
            "score ({}).".format(scores.min(), args.pass_above))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'filename', default='benchmark.txt', nargs='?',
        help="The collection that contains the benchmark data.")
    parser.add_argument(
        'filename_cmp', default='compare.txt', nargs='?')
    parser.add_argument(
        '-f', '--filter', type=str,
        help="Select a subset of the data.")
    parser.add_argument(
        '--pass-above',
        type=float,
        default=0.99,
        help="Specify a minimal score that we need to pass.")
    args = parser.parse_args()

    main(args)
