import signac

db = signac.get_database('testing')

with signac.Collection.open('benchmark.txt') as c:
    for doc in c:
        db.signac_benchmarks.replace_one(doc['meta'], doc, upsert=True)
