from tempfile import TemporaryDirectory
from functools import partial
from pprint import pprint
import random
import string

from tabulate import tabulate
import signac
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


if __name__ == '__main__':
    random.seed(0)
    headers = ['N', 'Metadata Size (total)', 'Category', 'Time / N [\u00B5]']
    rows = []
    for N in 10, 100, 1000:
        rows.append([N, None, None, None])
        for data_size in 100, 1000:
            with TemporaryDirectory() as tmp:
                project = signac.init_project('benchmark-N={}'.format(N), root=tmp)
                generate_random_data(project, N, data_size, data_size)

                size = pb.determine_project_size(project)
                rows.append([None, fmt_size(size['total']), None, None])

                data = pb.benchmark_project(project)
                for cat, values in data.items():
                    n, min_value = list(sorted(values, key=lambda x: x[1]))[0]
                    mean_min_value = 1e6 * min_value / n
                    rows.append([None, None, cat, mean_min_value])

    print(tabulate(rows, headers=headers))
