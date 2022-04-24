from unicodedata import name
import pandas as pd
from pymongo import MongoClient
import json

# mongodb url
client = MongoClient('')
# db name
db = client.political_data_analysis
# select collection
coll = db.testcollection2
# read csv file
data = pd.read_csv('test2.csv')
payload = json.loads(data.to_json(orient='records'))
coll.insert_many(payload)
