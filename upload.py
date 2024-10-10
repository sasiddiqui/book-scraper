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

# data = json.load(open("jsons/sifatusafwa.json"))
# data.append(json.load(open("jsons/co.json")))

# books.insert_many(data)
count = 1
for book in books.find({"source": "Qurtuba"}):
    print(count)
    books.update_one({"_id": book["_id"]}, {"$set": {"price": float(book["price"] * 1.33)}})
    count += 1

print("Inserted all books into the database")

