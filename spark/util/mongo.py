from pymongo import MongoClient

def write_to_mongo(df, collection, db="podcastDB"):
    client = MongoClient("mongodb://mongodb:27017")
    records = df.toPandas().to_dict("records")
    if records:
        client[db][collection].insert_many(records)
    client.close()