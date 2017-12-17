import os
import six
import random
import string
import timeit
import logging
import warnings
from multiprocessing import Pool
from collections import OrderedDict
from contextlib import contextmanager

from tqdm import tqdm
from signac.contrib.hashing import calc_id
import datreant.core as dtr

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


def setup(N, root=None):
    if root is None:
        root = '.'
    for i in tqdm(range(N)):
        sp = dict(a=i)
        s = dtr.Treant(os.path.join(root, 'workspace', calc_id(sp)))
        for key, value in sp.items():
            s.categories[key] = value


def determine_bundle_size(root):
    bundle = dtr.Bundle(os.path.join(root, 'workspace', '*'))
    size = 0
    for t in bundle:
        for c in t.children:
            size += os.path.getsize(str(c))
    return {
        'N': len(bundle),
        'total': size,
    }


def _random_str(size):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(size))


def _make_doc(i, num_keys=1, data_size=0):
    assert num_keys >= 1
    assert data_size >= 0

    doc = {'b_{}'.format(j): _random_str(data_size) for j in range(num_keys - 1)}
    doc['a'] = '{}{}'.format(i, _random_str(max(0, data_size - len(str(i)))))
    return doc


def _make_treant(root, num_keys, num_doc_keys, data_size, data_std, i):
    size = max(0, int(random.gauss(data_size, data_std)))
    sp = _make_doc(i, num_keys, size)
    _id = calc_id(sp)
    t = dtr.Treant(os.path.join(root, 'workspace', _id))
    t.categories = sp
    assert t.categories == sp


def generate_random_data(root, N_sp, num_keys=1, num_doc_keys=0,
                         data_size=0, data_std=0, parallel=True):
    if six.PY2:
        if parallel:
            warnings.warn("Function 'generate_random_data()' not parallelized for Python 2.")
            parallel = False

    if parallel:
        with Pool() as pool:
            p = [(root, num_keys, num_doc_keys, data_size, data_std, i) for i in range(N_sp)]
            list(pool.starmap(_make_treant, tqdm(p, desc='init random project data')))
    else:
        from functools import partial
        make = partial(_make_treant, root, num_keys, num_doc_keys, data_size, data_std)
        list(map(make, tqdm(range(N_sp), desc='init random project data')))


@contextmanager
def setup_random_bundle(N, num_keys=1, num_doc_keys=0,
                        data_size=0, data_std=0, seed=0, root=None):
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    with TemporaryDirectory(dir=root) as tmp:
        generate_random_data(tmp, N, num_keys, num_doc_keys, data_size, data_std)
        yield tmp


def benchmark_bundle(root, keys=None, skip_rich_filter=False):
    setup = "import datreant.core as dtr; bundle = dtr.Bundle('{}/workspace/*');".format(root)
    setup += "import random;"

    data = OrderedDict()

    def run(key, timer, repeat=3, number=10):
        if keys is None or key in keys:
            logger.info("Run '{}'...".format(key))
            data[key] = timer.repeat(repeat=repeat, number=number)

    run('determine_len', Timer('len(bundle)', setup=setup))

    run('select_by_id', Timer(
        stmt="dtr.Treant(path).categories",
        setup=setup + "path = random.choice(bundle)"))

    run('iterate', Timer(
        stmt='[dtr.Treant(b).categories for b in bundle]',
        setup=setup))

    run('iterate_single_pass', Timer(
        stmt='[dtr.Treant(b).categories for b in bundle]',
        setup=setup), number=1)

    run('search_lean_filter', Timer(
        stmt="bundle.categories.groupby(k)[v]",
        setup=setup + 'import random; sp = dtr.Treant(random.choice(bundle)).categories;'
        'k, v = dict(sp).popitem();'))

    if not skip_rich_filter:
        run('search_rich_filter', Timer(
            stmt="bundle.categories.groupby(keys)[tuple(values)];",
            setup=setup + "sp = dict(dtr.Treant(random.choice(bundle)).categories);"
                          "keys = list(sp); values = [sp[k] for k in keys];"))

    return data
