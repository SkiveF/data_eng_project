import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

train_path = "../raw_data/train.csv"

CHUNKSIZE = 100

index_name_train = "loan_prediction_train"
doc_type_train = "av-lp_train"

print(train_path)


def index_data(data_path, chunk_size, index_name, doc_type):
    f = open(data_path)
    df = pd.read_csv(f, iterator=True, chunksize=chunk_size)
    es = Elasticsearch('http://localhost:9200')
    # es.info().body()
    try:
        es.delete(index_name)
    except:
        pass
    es.index(index=index_name)
    for i, df in enumerate(df):
        records = df.where(pd.notnull(df), None).T.to_dict()
        list_records = [records[it] for it in records]
        try:
            es.bulk(index_name, doc_type, list_records)
        except:
            print("error !, skiping chunk !")
            pass


index_data(train_path, CHUNKSIZE, index_name_train, doc_type_train)

