import sys
import random
import string
import platform
import base64
import json
from tempfile import TemporaryDirectory, NamedTemporaryFile
from cProfile import Profile
from contextlib import contextmanager
from itertools import product

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
    parser.add_argument('cmd', choices=['run', 'report', 'plot'])
    parser.add_argument(
        '-N', nargs='+', type=int, default=[10, 100, 1000, 10000])
    parser.add_argument(
        '-k', '--num-keys', type=int, nargs='+', default=[10])
    parser.add_argument(
        '--num-doc-keys', type=int, nargs='+', default=[10])
    parser.add_argument(
        '-s', '--data-size', type=int, default=[100])
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
            'num-per-minute',   # number of operations per minute
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
                        'python': sys.version_info,
                        'signac': signac.__version__,
                    }}}
            key = doc.copy()
            key['profile'] = {'$exists': args.profile}

            if not args.overwrite:
                with Collection.open('benchmark.txt') as c:
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

                with Collection.open('benchmark.txt') as c:
                    c.replace_one(key, doc, upsert=True)

    elif args.cmd == 'report':
        assert args.style == 'per-N'
        headers = ['N', 'Size', 'Category', 'Time  / N [\u00B5s]']
        rows = []
        if args.db:
            db = signac.get_database('testing')
            c = db.signac_benchmarks
            docs = list(sorted(c.find(q_reports), key=key_reports))
        else:
            with Collection.open('benchmark.txt') as c:
                docs = list(sorted(c.find(q_reports), key=key_reports))

        for doc in docs:
            for i, (cat, values) in enumerate(doc['data'].items()):
                n, min_value = list(sorted(values, key=lambda x: x[1]))[0]
                mean_min_value = 1e6 * min_value / n / doc['meta']['N']
                if i:
                    rows.append([None, None, cat, mean_min_value])
                else:
                    rows.append(
                        [doc['meta']['N'], fmt_size(doc['size']['total']),
                         cat, mean_min_value])
        print(tabulate(rows, headers=headers))

    elif args.cmd == 'plot':
        import numpy as np
        from matplotlib import pyplot as plt

        q_reports['N'] = {'$gte': 100}

        fig, ax = plt.subplots()

        def fmt_meta(meta):
            return "{} ({})".format(meta['N'], fmt_size(meta['size']['total']))
            return "N={}/{}/{} ({})".format(
                meta['N'], meta['num_keys'], meta['num_meta_keys'], fmt_size(meta['size']['total']))

        def calc_means(doc):
            for cat in sorted(doc['data']):
                values = doc['data'][cat]
                n, min_value = list(sorted(values, key=lambda x: x[1]))[0]
                if args.style in ('per-N', 'factor'):
                    yield cat, 1e6 * min_value / n / doc['N']
                if args.style in ('per-size'):
                    yield cat, 1e6 * min_value / n / doc['size']['total']
                elif args.style == 'absolute':
                    yield cat, min_value / n
                elif args.style == 'num-per-minute':
                    print(cat, 1.0 / (min_value / n));
                    yield cat, 1.0 / (60 * min_value / n)
                else:
                    raise NotImplementedError(args.style)

        with Collection.open('benchmark.txt') as c:
            docs = list(sorted(c.find(q_reports), key=key_reports))
            if not docs:
                raise RuntimeError("No data!")

            cats = list(sorted(docs[0]['data']))

            width = 1.0
            N = len(docs)
            ind = np.arange(N)
            data = []
            minima = dict()
            xtics = []

            if args.style == 'factor':
                for doc in docs:
                    for cat, mean_min_value in calc_means(doc):
                        if cat in minima:
                            minima[cat] = min(minima[cat], mean_min_value)
                        else:
                            minima[cat] = mean_min_value
            else:
                minima = {cat: 1.0 for cat in cats}

            data = []
            for doc in docs:
                data.append([mmv / minima[cat] for cat, mmv in calc_means(doc)])

            x = np.array(data)
            M = len(x.T)
            w = width / M
            p = []
            for i, m in enumerate(x.T):
                p.append(ax.bar(1.2 * ind + width * ((i + 0.5) / M - 0.5), m, 0.8 * w))

            ax.set_xticks(1.2 * ind)
            ax.set_xticklabels([fmt_meta(doc['meta']) for doc in docs], rotation=0)
            if args.style == 'factor':
                ax.set_ylabel("X")
            elif args.style == 'absolute':
                ax.set_ylabel("Time [s]")
            elif args.style == 'per-N':
                ax.set_ylabel("Time / N [\u00B5s]")
            elif args.style == 'per-size':
                ax.set_ylabel("Time / size [\u00B5s/Bytes]")
            elif args.style == 'num-per-minute':
                ax.set_ylabel("Per Second [s-1]")
            else:
                raise NotImplementedError(args.style)
            ax.legend([p_[0] for p_ in p], list(map(tr, cats)))
            fig.tight_layout()
            plt.show()

    else:
        raise ValueError("Illegal command: '{}'.".format(args.cmd))
