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

data = json.load(open("jsons/buraq.json"))

# ensure every book has the following keys: url, source, title, price, image, instock otherwise remove it
req = ["url", "source", "title", "price", "image", "instock"]
length = len(data)

for book in data:
    if not all(key in book for key in req):
        data.remove(book)

a = input(f"Removed {length - len(data)} books from the list of {length} books. Do you want to continue? (y/n) ")
if a.lower() != "y":
    print("Exiting...")
    exit()


books.insert_many(data)

# count = 1
# for book in books.find({"source": "Qurtuba"}):
#     print(count)
#     books.update_one({"_id": book["_id"]}, {"$set": {"price": float(book["price"] * 1.33)}})
#     count += 1

print("Inserted all books into the database")

