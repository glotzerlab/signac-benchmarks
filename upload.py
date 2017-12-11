import signac
from signac.contrib.collection import _traverse_tree

db = signac.get_database('testing')

with signac.Collection.open('benchmark.txt') as c:
    for doc in c:
        del doc['_id']
        key = dict(_traverse_tree(doc['meta'], key='meta'))
        db.signac_benchmarks.replace_one(key, doc, upsert=True)
