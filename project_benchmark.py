import os
import timeit
from collections import OrderedDict
import logging

from tqdm import tqdm

logger = logging.getLogger(__name__)


class Timer(timeit.Timer):

    def timeit(self, number=10):
        return number, super().timeit(number=number)

    def repeat(self, repeat=3, number=10):
        return super().repeat(repeat=repeat, number=number)


def size(fn):
    try:
        return os.path.getsize(fn)
    except FileNotFoundError:
        return 0


def calc_project_metadata_size(project):
    sp_size = []
    doc_size = []
    for job in tqdm(project, 'determine metadata size'):
        sp_size.append(size(job.fn(job.FN_MANIFEST)))
        doc_size.append(size(job.fn(job.FN_DOCUMENT)))
    return sp_size, doc_size


def fmt_size(size, units=None):
    "Returns a human readable string reprentation of bytes."
    if units is None:
        units = [' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    return str(size) + units.pop(0) if size < 1024 else fmt_size(size >> 10, units[1:])


def total(benchmarks):
    n = sum((b[0] for b in benchmarks))
    dt = sum((b[1] for b in benchmarks))
    return n, dt


def print_result(n, dt):
    print("Mean time: {:.2}s ({} iterations)".format(dt / n, n))


def determine_project_size(project):
    sp_size, doc_size = calc_project_metadata_size(project)
    meta = {
        'N': len(project),
        'statepoint_metadata_size': sum(sp_size),
        'document_metadata_size': sum(doc_size),
        'total': sum(sp_size) + sum(doc_size),
    }
    return meta


def benchmark_project(project, keys=None):
    root = project.root_directory()
    setup = "import signac; project = signac.get_project(root='{}'); ".format(root)
    setup += "from itertools import islice, repeat; import random; "

    data = OrderedDict()

    def run(key, timer, repeat=3, number=10):
        if keys is None or key in keys:
            logger.info("Run '{}'...".format(key))
            data[key] = timer.repeat(repeat=repeat, number=number)

    run('determine_len', Timer('len(project)', setup=setup))

    run('select_job_by_id', Timer(
            stmt="project.open_job(id=jobid)",
            setup=setup+"jobid = random.choice(list(islice(project, 100))).get_id()"))

    run('iterate', Timer("list(project)", setup), 3, 10)

    run('search_lean_filter', Timer(
            stmt="len(project.find_jobs(f))",
            setup=setup+"sp = project.open_job(id=random.choice(list(project.find_job_ids()))).sp();"
                        "k, v = sp.popitem(); f = {k: v}"))

    run('search_rich_filter', Timer(
            stmt="len(project.find_jobs(f))",
            setup=setup+"f = project.open_job(id=random.choice(list(project.find_job_ids()))).sp()"))

    return data
