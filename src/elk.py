import io
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# connexion
es = Elasticsearch("https://localhost:9200",
                   http_auth=('elastic', 'rnlr2x1qO5SF3d*IgbVP'),
                   verify_certs=False)

print(es.ping())

# create one index(part 1)
# es.indices.create(index="tuto-sf_15")


# create multi index with sequence(part 2)
# index_basename = "juin"
# i = 1
# for i in range(11):
#     response = es.indices.create(index=index_basename + "_" + str(i))
#     print(response)


# create index in bulk using input file
# with io.open("../raw_data/input.txt", "r", encoding="utf-8") as f:
#     data = f.read()
#     f.close()
# data = data.split("\n")
# for index in data:
#     response = es.indices.create(index=index)
#     print(response)

# search elasticsearch index (part 3)
index = "hr"
try:
    research = es.search(index=index)
    print(research["_shards"]["total"])
except Exception as e:
    print(str(e))

# research index base on pattern
index = "juin_*"
try:
    response = es.search(index=index)
    print(response["_shards"]["total"])
except Exception as e:
    print(str(e))

# show all indices or delete
indices = es.indices.get_alias(index=index)
if len(indices) > 0:
    for index in indices:
        delete_res = es.indices.delete(index=index)
        print(delete_res)
else:
    print("No index has been found for the given search !")
