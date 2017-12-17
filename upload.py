import argparse

import signac
from signac.contrib.collection import _traverse_tree

parser = argparse.ArgumentParser()
parser.add_argument(
    'filename', nargs='?', default='benchmark.txt',
    help="The name of the benchmark collection file.")
args = parser.parse_args()


db = signac.get_database('testing')

with signac.Collection.open(args.filename) as c:
    for doc in c:
        del doc['_id']
        key = dict(_traverse_tree(doc['meta'], key='meta'))
        key['profile'] = {'$ne' if doc.get('profile') else '$eq': None}
        db.signac_benchmarks.replace_one(key, doc, upsert=True)
