#!/usr/bin/env python
import sys
import logging
import random
import argparse
import platform
import base64
import json
from pprint import pprint
from cProfile import Profile
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from tempfile import gettempdir

from signac import Collection

from util import fmt_size
from util import get_partition


def expected_size(args):
    return args.N * args.data_size * (args.num_keys + args.num_doc_keys)


def default_doc(args):
    tmpdir = gettempdir() if args.root is None else args.root
    return {'meta': {
        'tool': args.tool,
        'N': args.N,
        'num_keys': args.num_keys,
        'num_doc_keys': args.num_doc_keys,
        'data_size': args.data_size,
        'seed': args.seed,
        'cached': args.cached,
        'categories': args.categories,
        'platform': platform.uname()._asdict(),
        'fstype': get_partition(tmpdir).fstype,
    }}


@contextmanager
def run_with_profile():
    profile = Profile()
    profile.enable()
    yield profile
    profile.disable()
    with NamedTemporaryFile() as statsfile:
        profile.dump_stats(statsfile.name)
        statsfile.flush()
        statsfile.seek(0)
        profile.stats = base64.b64encode(statsfile.read()).decode()


def benchmark_signac(args, check_skip, store_result):
    import signac
    from benchmark_signac import setup_random_project
    from benchmark_signac import determine_project_size
    from benchmark_signac import benchmark_project

    doc = default_doc(args)
    doc['meta']['versions'] = {
        'python': '.'.join(map(str, sys.version_info)),
        'signac': signac.__version__}
    key = doc.copy()
    key['profile'] = {'$exists': args.profile}

    if check_skip(key):
        return

    with setup_random_project(args.N, args.num_keys, args.num_doc_keys,
                              data_size=args.data_size, data_std=args.data_std,
                              seed=args.seed, root=args.root) as project:
        if args.cached:
            project.update_cache()
        doc['size'] = determine_project_size(project)
        if args.profile:
            with run_with_profile() as profile:
                doc['data'] = benchmark_project(project, args.categories)
            doc['profile'] = profile.stats
        else:
            doc['data'] = benchmark_project(project, args.categories)

    store_result(key, doc)


def benchmark_datreant_core(args, check_skip, store_result):
    import datreant.core as dtr
    from benchmark_datreant import setup_random_bundle
    from benchmark_datreant import determine_bundle_size
    from benchmark_datreant import benchmark_bundle

    doc = default_doc(args)
    doc['meta']['versions'] = {
        'python': '.'.join(map(str, sys.version_info)),
        'datreant.core': dtr.__version__}
    key = doc.copy()
    key['profile'] = {'$exists': args.profile}

    if check_skip(key):
        return

    with setup_random_bundle(
            args.N, args.num_keys, args.num_doc_keys,
            data_size=args.data_size, data_std=args.data_std,
            seed=args.seed, root=args.root) as bundle:
        assert not args.cached
        doc['size'] = determine_bundle_size(bundle)
        if args.profile:
            with run_with_profile() as profile:
                doc['data'] = benchmark_bundle(bundle, args.categories)
            doc['profile'] = profile.stats
        else:
            doc['data'] = benchmark_bundle(bundle, args.categories)

    store_result(key, doc)


def main(args):
    random.seed(args.seed)

    if args.dry_run:
        print("Expected size:", fmt_size(int(expected_size(args))))
        return

    def check_skip(key):
        if not args.overwrite or args.output == '-':
            with Collection.open(args.output) as c:
                if len(c.find(key)) >= 1:
                    print("Already ran.")
                    return True
        return False

    def store_result(key, doc):
        if args.output != '-':
            with Collection.open(args.output) as c:
                c.replace_one(key, doc, upsert=True)

    if args.tool == 'signac':
        benchmark_signac(args, check_skip, store_result)
    elif args.tool == 'datreant.core':
        benchmark_datreant_core(args, check_skip, store_result)
    else:
        raise ValueError("Unknown tool '{}'.".format(args.tool))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'tool', choices=['signac', 'datreant.core'], nargs='?', default='signac',
        help="Specify which data management tool to benchmark.")
    parser.add_argument(
        '-o', '--output', nargs='?', default='benchmark.txt',
        help="Specify which collection file to store results to or '-' for None.")
    parser.add_argument(
        '-N', type=int, default=100,
        help="The number of data/ state points within the benchmarked project.")
    parser.add_argument(
        '-k', '--num-keys', type=int, default=10,
        help="The numnber of primary metadata keys.")
    parser.add_argument(
        '--num-doc-keys', type=int, default=0,
        help="The number of secondary metadata keys (if applicable).")
    parser.add_argument(
        '-s', '--data-size', type=int, default=100,
        help="The mean data size")
    parser.add_argument(
        '--data-std', type=float, default=0,
        help="The standard deviation of the data size.")
    parser.add_argument(
        '-r', '--seed', type=int, default=0,
        help="The random seed to use.")
    parser.add_argument(
        '--cached', action='store_true',
        help="Use caching option if applicable.")
    parser.add_argument(
        '-p', '--profile', action='store_true',
        help="Activate profiling (Results should not be used for reporting.")
    parser.add_argument(
        '--overwrite', action='store_true',
        help="Overwrite existing result.")
    parser.add_argument(
        '-n', '--dry-run', action='store_true',
        help="Perform a dry run, do not actually benchmark.")
    parser.add_argument(
        '--root', type=str,
        help="Specify the root directory for all temporary directories. "
             "Defaults to the system default temp directory.")
    parser.add_argument(
        '-c', '--categories', nargs='+',
        help="Limit benchmark to given categories.")
    parser.add_argument(
        '--debug', action='store_true',
        help="Activate debug logging output.")
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    main(args)
