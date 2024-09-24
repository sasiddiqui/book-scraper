import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = open("mongourl.txt").read().strip()

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

client.admin.command('ping')
print("Pinged your deployment. You successfully connected to MongoDB!")

db = client["data"]
books = db["books"]

data = json.load(open("jsons/qurtuba.json"))
# data.append(json.load(open("jsons/co.json")))

books.insert_many(data)

print("Inserted all books into the database")

