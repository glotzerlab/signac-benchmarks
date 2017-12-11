from tempfile import TemporaryDirectory
from functools import partial
from pprint import pprint
import random
import string

from tabulate import tabulate
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
    for i in range(N):
        project.open_job(make_doc(i, sp_size)).init()
    if doc_size is not None:
        for i, job in enumerate(project):
            job.document.update(make_doc(i, doc_size))


def run_benchmark(N, data_size, seed=0, n=1):
    meta = {
        'N': N, 'data_size': data_size,
        'version': signac.__version__, 'seed': seed
    }
    with Collection.open('benchmark.txt') as c:
        if len(c.find(meta)) >= n:
            return
    random.seed(0)
    with TemporaryDirectory() as tmp:
        project = signac.init_project('benchmark-N={}'.format(N), root=tmp)
        generate_random_data(project, N, data_size, data_size)
        size = pb.determine_project_size(project)
        data = {'data': pb.benchmark_project(project)}
        data['size'] = size
        data.update(meta)
        with Collection.open('benchmark.txt') as c:
            c.replace_one(meta, data, upsert=True)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', choices=['run', 'report'])
    args = parser.parse_args()
    random.seed(0)

    if args.cmd == 'run':
        for N in 10, 100, 1000:
            for data_size in 100, 1000:
                run_benchmark(N, data_size)
    elif args.cmd == 'report':
        headers = ['N', 'Size', 'Category', 'Time [\u00B5]']
        rows = []
        with Collection.open('benchmark.txt') as c:
            docs = list(sorted(c, key=lambda doc: (doc['N'], doc['data_size'])))
            for doc in docs:
                for i, (cat, values) in enumerate(doc['data'].items()):
                    n, min_value = list(sorted(values, key=lambda x: x[1]))[0]
                    mean_min_value = 1e6 * min_value / n
                    if i:
                        rows.append([None, None, cat, mean_min_value])
                    else:
                        rows.append(
                            [doc['N'], fmt_size(doc['size']['total']),
                            cat, mean_min_value])
        print(tabulate(rows, headers=headers))
    else:
        raise ValueError("Illegal command: '{}'.".format(args.cmd))

