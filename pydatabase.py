##################################################
# Python Example using MongoDB Database
#

import pprint
import pymongo

from pymongo import MongoClient

# Connect to a locally hosted MongoDB database 'testdb' with a collection 'eateries'
client = pymongo.MongoClient(host='localhost')
#client = MongoClient('localhost', 27017)

# Access the 'eateries' collection in the 'testdb' database
collection = client.testdb.eateries

new_documents = [
    {
        "name": "Frankies Trattoria",
        "stars": 4,
        "categories": ["Italian", "Pizza", "Pasta", "Bakery"]
    },
    {
        "name": "Gran's Deli",
        "stars": 5,
        "categories": ["Bakery", "Cakes", "Salads", "Sandwiches"]
    },
    {
        "name": "The Greasy Spoon",
        "stars": 3,
        "categories": ["Pies", "Burgers", "Chips", "Bakery"]
    }
]

# Clear the collection
collection.remove()

# Insert items into the collection
collection.insert_many(new_documents)

# Query
for eatery in collection.find():
    pprint.pprint(eatery)

# Create Index
collection.create_index([('name', pymongo.ASCENDING)])

# Perform aggregation
pipeline = [
    {"$match": {"categories": "Bakery"}},
    {"$group": {"_id": "$stars", "count": {"$sum": 1}}}
]

pprint.pprint(list(collection.aggregate(pipeline)))
