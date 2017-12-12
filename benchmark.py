import sys
import random
import string
import platform
import base64
import json
from tempfile import TemporaryDirectory, NamedTemporaryFile
from cProfile import Profile
from pstats import Stats
from contextlib import contextmanager
from itertools import product, groupby

from tabulate import tabulate
from tqdm import tqdm
import signac
from signac import Collection
import project_benchmark as pb


def fmt_size(size, units=None):
    "Returns a human readable string reprentation of bytes."
    if units is None:
        units = [' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    return str(size) + units.pop(0) if size < 1024 else fmt_size(size >> 10, units[1:])


def _random_str(size):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(size))


def make_doc(i, num_keys=1, data_size=0):
    assert num_keys >= 1
    assert data_size >= 0

    doc = {'b_{}'.format(j): _random_str(data_size) for j in range(num_keys - 1)}
    doc['a'] = '{}{}'.format(i, _random_str(max(0, data_size - len(str(i)))))
    return doc


def generate_random_data(project, N_sp, num_keys=1, num_doc_keys=0, data_size=0, data_std=0):
    assert len(project) == 0

    def make(i):
        size = max(0, int(random.gauss(data_size, data_std)))
        job = project.open_job(make_doc(i, num_keys, size))
        if num_doc_keys > 0:
            size = max(0, int(random.gauss(data_size, data_std)))
            job.document.update(make_doc(i, num_doc_keys, size))
        else:
            job.init()

    list(map(make, tqdm(range(N_sp), desc='generate random project data')))


@contextmanager
def setup_project(N, num_keys, num_doc_keys, data_size, data_std, seed=0, root=None):
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    with TemporaryDirectory(dir=root) as tmp:
        project = signac.init_project('benchmark-N={}'.format(N), root=tmp)
        generate_random_data(project, N, num_keys, num_doc_keys, data_size, data_std)
        yield project


def tr(s):
    return {
        'determine_len': "Determine project size",
        'index_100': "Generate 100 index entries",
        'iterate_100': "Iterate through 100 jobs",
        'search_rich_filter': "Search with rich filter",
        'search_lean_filter': "Search with lean filter",
        'select_job_by_id': "Select job by id",
    }.get(s, s)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', choices=['run', 'report', 'plot', 'pstats'])
    parser.add_argument('filename', nargs='?', default='benchmark.txt')
    parser.add_argument(
        '-N', nargs='+', type=int, default=[100, 1000, 10000])
    parser.add_argument(
        '-k', '--num-keys', type=int, nargs='+', default=[10])
    parser.add_argument(
        '--num-doc-keys', type=int, nargs='+', default=[10])
    parser.add_argument(
        '-s', '--data-size', type=int, nargs='+', default=[100])
    parser.add_argument('--data-std', type=float, default=25)
    parser.add_argument('-p', '--profile', action='store_true')
    parser.add_argument('-r', '--seed', type=int, default=0)
    parser.add_argument('-c', '--categories', nargs='+')
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('-n', '--dry-run', action='store_true')
    parser.add_argument('-q', '--query', type=str)
    parser.add_argument('--root', type=str)
    parser.add_argument(
        '--style', choices=[
            'absolute',         # absolute time in seconds
            'per-N',            # divided by the number of project state points
            'per-size',         # divided by the total metadata size
            'factor',           # divided by the minimal time
            'inverted',   # number of operations per minute
        ], default='per-N')
    parser.add_argument('--db', action='store_true')
    args = parser.parse_args()

    q_reports = {'profile': {'$exists': False}, 'meta.categories': None}
    if args.query:
        q_reports.update(json.loads(args.query))

    def key_reports(doc):
        return [doc['meta'][k] for k in ('N', 'num_keys', 'num_doc_keys', 'data_size')]

    if args.cmd == 'run':
        for N, num_keys, num_doc_keys, data_size in product(
                args.N, args.num_keys, args.num_doc_keys, args.data_size):
            assert all(isinstance(_, int) for _ in (N, data_size, num_keys, num_doc_keys))
            doc = {'meta': {
                'N': N,
                'num_keys': num_keys,
                'num_doc_keys': num_doc_keys,
                'data_size': data_size,
                'seed': args.seed,
                'categories': args.categories,
                'platform': platform.uname()._asdict(),
                'versions': {
                    'python': '.'.join(map(str, sys.version_info)),
                    'signac': signac.__version__,
                }}}
            key = doc.copy()
            key['profile'] = {'$exists': args.profile}

            if not args.overwrite:
                with Collection.open(args.filename) as c:
                    if len(c.find(key)) >= 1:
                        continue   # already run
            expected_size = N * data_size * (num_keys + num_doc_keys)
            if args.dry_run:
                print("Expected size:", fmt_size(int(expected_size)))
                continue
            with setup_project(N, num_keys, num_doc_keys,
                               data_size=data_size, data_std=args.data_std,
                               seed=args.seed, root=args.root) as project:
                doc['size'] = pb.determine_project_size(project)

                if args.profile:
                    profile = Profile()
                    profile.enable()
                    doc['data'] = pb.benchmark_project(project, args.categories)
                    profile.disable()
                    with NamedTemporaryFile() as statsfile:
                        profile.dump_stats(statsfile.name)
                        statsfile.flush()
                        statsfile.seek(0)
                        doc['profile'] = base64.b64encode(statsfile.read()).decode()
                else:
                    doc['data'] = pb.benchmark_project(project, args.categories)

                with Collection.open(args.filename) as c:
                    c.replace_one(key, doc, upsert=True)

    elif args.cmd in ('report', 'plot'):
        import numpy as np

        if args.db:
            db = signac.get_database('testing')
            c = db.signac_benchmarks
            docs = list(sorted(c.find(q_reports), key=key_reports))
        else:
            with Collection.open(args.filename) as c:
                docs = list(sorted(c.find(q_reports), key=key_reports))
        if not docs:
            raise RuntimeError("No data!")

        cats = list(sorted(docs[0]['data']))

        def group_key(doc):
            return doc['meta']['N'], doc['size']['total']

        def mean(doc, cat):
            n, min_value = list(sorted(doc['data'][cat], key=lambda x: x[1]))[0]
            if args.style == 'per-N':
                return 10e3 * min_value / n / doc['meta']['N']
            elif args.style == 'per-size':
                return 10e3 * min_value / n / (doc['size']['total'] / 1024)
            elif args.style == 'absolute':
                return min_value / n
            elif args.style == 'inverted':
                return n / min_value / 1000
            else:
                raise NotImplementedError(args.style)

        label = {
            'absolute': "Time [s]",
            'per-N': "Time / N [ms]",
            'per-size': "Time / Size [ms/kB]",
            'inverted': "1k OPs per second [1/s]",
        }[args.style]

        def calc_means(group):
            group = list(group)
            for cat in cats:
                data = [mean(doc, cat) for doc in group]
                yield cat, np.mean(data)

        if args.cmd == 'report':
            headers = ['N', 'Size', 'Category', label]
            rows = []

            for (N, total), group in groupby(docs, key=group_key):
                for i, (cat, mmv) in enumerate(calc_means(group)):
                    if i:
                        rows.append([None, None, cat, mmv])
                    else:
                        rows.append([N, fmt_size(total), cat, mmv])
            print(tabulate(rows, headers=headers))

        elif args.cmd == 'plot':
            from matplotlib import pyplot as plt

            fig, ax = plt.subplots()

            def fmt_doc(doc):
                return "N={} ({})".format(doc['meta']['N'], fmt_size(doc['size']['total']))

            width = 1.0
            data = []
            minima = dict()
            xtics = []

            if args.style == 'factor':
                raise NotImplementedError()
                for doc in docs:
                    for cat, mean_min_value in calc_means(doc):
                        if cat in minima:
                            minima[cat] = min(minima[cat], mean_min_value)
                        else:
                            minima[cat] = mean_min_value
            else:
                minima = {cat: 1.0 for cat in cats}

            data = []
            for key, group in groupby(docs, key=fmt_doc):
                data.append([mmv / minima[cat] for cat, mmv in calc_means(group)])
            x = np.array(data)
            M = len(x.T)
            w = width / M
            p = []
            ind = np.arange(len(x))
            for i, m in enumerate(x.T):
                p.append(ax.bar(1.2 * ind + width * ((i + 0.5) / M - 0.5), m, 0.8 * w))

            ax.set_xticks(1.2 * ind)
            ax.set_xticklabels([key for key, _ in groupby(docs, key=fmt_doc)])
            ax.set_ylabel(label)
            ax.legend([p_[0] for p_ in p], list(map(tr, cats)))
            fig.tight_layout()
            plt.show()

    elif args.cmd == 'pstats':
        stats = Stats()
        with Collection.open('benchmark.txt') as c:
            for doc in c.find({'profile': {'$ne': None}}):
                with NamedTemporaryFile() as tmp:
                    tmp.write(base64.b64decode(doc['profile'].encode()))
                    tmp.flush()
                    stats.add(tmp.name)
        stats.sort_stats('cumtime')
        stats.print_stats()

    else:
        raise ValueError("Illegal command: '{}'.".format(args.cmd))
