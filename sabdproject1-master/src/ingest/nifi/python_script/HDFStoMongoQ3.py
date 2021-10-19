import sys
from pymongo import MongoClient  # pip install pymongo
import pandas as pd


def mongo_put(my_data_dict, collection_name, db_name):
    # Create connection to MongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client[db_name]
    collection = db[collection_name]

    for item in my_data_dict:
        collection.insert(item)


def get_db_param():
    if len(sys.argv) != 3:
        sys.stdout("Usage: collection name, db name")
        return -1

    db_name = str(sys.argv[1])
    collection_name = str(sys.argv[2])

    return collection_name, db_name


def main():

    csv_file = sys.stdin

    col_names = ["Month", "State", "Cluster"]
    df = pd.read_csv(csv_file, header=None, names=col_names)
    data_dict = df.to_dict("records")
    collection_name, db_name = get_db_param()
    mongo_put(data_dict, collection_name, db_name)

    return 0


if __name__ == '__main__':
    main()
