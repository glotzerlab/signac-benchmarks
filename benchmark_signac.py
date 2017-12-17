import os
import six
import string
import random
import timeit
import warnings
import logging
from contextlib import contextmanager
from collections import OrderedDict
from multiprocessing import Pool

import signac
from tqdm import tqdm

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


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


def noop(*args, **kwarg):
    pass


def _random_str(size):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(size))


def _make_doc(i, num_keys=1, data_size=0):
    assert num_keys >= 1
    assert data_size >= 0

    doc = {'b_{}'.format(j): _random_str(data_size) for j in range(num_keys - 1)}
    doc['a'] = '{}{}'.format(i, _random_str(max(0, data_size - len(str(i)))))
    return doc


def _make_job(project, num_keys, num_doc_keys, data_size, data_std, i):
    size = max(0, int(random.gauss(data_size, data_std)))
    job = project.open_job(_make_doc(i, num_keys, size))
    if num_doc_keys > 0:
        size = max(0, int(random.gauss(data_size, data_std)))
        job.document.update(_make_doc(i, num_doc_keys, size))
    else:
        job.init()


def generate_random_data(project, N_sp, num_keys=1, num_doc_keys=0,
                         data_size=0, data_std=0, parallel=True):
    assert len(project) == 0

    if six.PY2:
        if parallel:
            warnings.warn("Function 'generate_random_data()' not parallelized for Python 2.")
            parallel = False

    if parallel:
        with Pool() as pool:
            p = [(project, num_keys, num_doc_keys, data_size, data_std, i) for i in range(N_sp)]
            list(pool.starmap(_make_job, tqdm(p, desc='init random project data')))
    else:
        from functools import partial
        make = partial(_make_job, project, num_keys, num_doc_keys, data_size, data_std)
        list(map(make, tqdm(range(N_sp), desc='init random project data')))


@contextmanager
def setup_random_project(N, num_keys=1, num_doc_keys=0,
                         data_size=0, data_std=0, seed=0, root=None):
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    with TemporaryDirectory(dir=root) as tmp:
        project = signac.init_project('benchmark-N={}'.format(N), root=tmp)
        generate_random_data(project, N, num_keys, num_doc_keys, data_size, data_std)
        yield project


def benchmark_project(project, keys=None):
    root = project.root_directory()
    setup = "import signac; project = signac.get_project(root='{}'); ".format(root)
    setup += "from itertools import islice, repeat; import random; "
    setup += "from benchmark_signac import noop;"
    #setup_parallel = setup + "from multiprocessing import Pool; pool = Pool();"

    data = OrderedDict()

    def run(key, timer, repeat=3, number=10):
        if keys is None or key in keys:
            logger.info("Run '{}'...".format(key))
            data[key] = timer.repeat(repeat=repeat, number=number)

    run('determine_len', Timer('len(project)', setup=setup))

    run('select_by_id', Timer(
        stmt="project.open_job(id=jobid)",
        setup=setup + "jobid = random.choice(list(islice(project, 100))).get_id()"))

    run('iterate', Timer("list(project)", setup), 3, 10)

    run('iterate_single_pass', Timer("list(project)", setup), number=1)

    #run('N_iterate', Timer("list(map(noop, project))", setup))
    #run('N_iterate_parallel', Timer("list(pool.map(noop, project))", setup_parallel))

    run('search_lean_filter', Timer(
        stmt="len(project.find_jobs(f))",
        setup=setup + "sp = project.open_job(id=random.choice(list(project.find_job_ids()))).sp();"
        "k, v = sp.popitem(); f = {k: v}"))

    run('search_rich_filter', Timer(
        stmt="len(project.find_jobs(f))",
        setup=setup + "f = project.open_job(id=random.choice(list(project.find_job_ids()))).sp()"))

    return data
