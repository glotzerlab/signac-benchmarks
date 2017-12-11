import sys
import random
import string
import base64
from tempfile import TemporaryDirectory, NamedTemporaryFile
from cProfile import Profile
from contextlib import contextmanager
from multiprocessing import Pool

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


def make_doc(i, data_size=None):
    if data_size is None:
        data = str(i)
    else:
        data = ''.join(random.choice(string.ascii_lowercase) for _ in range(data_size))
    return {str(i): data}


def generate_random_data(project, N_sp, sp_size=None, doc_size=None):
    assert len(project) == 0

    def make(i):
        job = project.open_job(make_doc(i, sp_size))
        if doc_size:
            job.document.update(make_doc(i, doc_size))
        else:
            job.init()

    list(map(make, tqdm(range(N_sp), desc='generate random project data')))


@contextmanager
def setup_project(N, data_size, seed=0):
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    with TemporaryDirectory() as tmp:
        project = signac.init_project('benchmark-N={}'.format(N), root=tmp)
        generate_random_data(project, N, data_size, data_size)
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
    parser.add_argument('-N', nargs='+', type=int, default=[10, 100, 1000, 1e4, 1e5, 1e6])
    parser.add_argument('-s', '--data-size', nargs='+', type=int, default=[100, 1000])
    parser.add_argument('-p', '--profile', action='store_true')
    parser.add_argument('--min-factor', action='store_true')
    parser.add_argument('--absolute', action='store_true')
    parser.add_argument('-r', '--seed', type=int, default=0)
    parser.add_argument('-c', '--categories', nargs='+')
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()

    q_reports = {'profile': {'$exists': False}, 'categories': None}
    key_reports=lambda doc: (doc['N'], doc['data_size'])

    if args.cmd == 'run':
        for N in [int(N) for N in args.N]:
            for data_size in args.data_size:
                random.seed(args.seed)
                doc = {'N': N, 'data_size': data_size,
                        'seed': args.seed, 'version': signac.__version__,
                        'categories': args.categories}
                key = doc.copy()
                key['profile'] = {'$exists': args.profile}

                if not args.overwrite:
                    with Collection.open('benchmark.txt') as c:
                        if len(c.find(key)) >= 1:
                            continue   # already run
                with setup_project(N, data_size, args.seed) as project:
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
        assert not args.min_factor
        assert not args.absolute
        headers = ['N', 'Size', 'Category', 'Time  / N [\u00B5s]']
        rows = []
        with Collection.open('benchmark.txt') as c:
            docs = list(sorted(c.find(q_reports), key=key_reports))
            for doc in docs:
                for i, (cat, values) in enumerate(doc['data'].items()):
                    n, min_value = list(sorted(values, key=lambda x: x[1]))[0]
                    mean_min_value = 1e6 * min_value / n / doc['N']
                    if i:
                        rows.append([None, None, cat, mean_min_value])
                    else:
                        rows.append(
                            [doc['N'], fmt_size(doc['size']['total']),
                             cat, mean_min_value])
        print(tabulate(rows, headers=headers))

    elif args.cmd == 'plot':
        import numpy as np
        from matplotlib import pyplot as plt

        q_reports['N'] = {'$gte': 100}

        fig, ax = plt.subplots()

        def fmt_meta(doc):
            return "N={} ({})".format(doc['N'], fmt_size(doc['size']['total']))

        def calc_means(doc):
            for cat in sorted(doc['data']):
                values = doc['data'][cat]
                n, min_value = list(sorted(values, key=lambda x: x[1]))[0]
                if args.absolute:
                    yield cat, min_value / n
                else:
                    yield cat, 1e6 * min_value / n / doc['N']

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

            if args.min_factor:
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
            ax.set_xticklabels([fmt_meta(doc) for doc in docs], rotation=15)
            if args.min_factor:
                ax.set_ylabel("X")
            elif args.absolute:
                ax.set_ylabel("Time [s]")
            else:
                ax.set_ylabel("Time / N [\u00B5s]")
            ax.legend([p_[0] for p_ in p], list(map(tr, cats)))
            fig.tight_layout()
            plt.show()

    else:
        raise ValueError("Illegal command: '{}'.".format(args.cmd))
