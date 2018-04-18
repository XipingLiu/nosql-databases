# Homework 3 - Xiping Liu (xl2639)
# How to run this script: python3 homework_3.py

# I used the following command to import movies.bson
# mongorestore -d myDB -c movies movies.bson

import pymongo
from pymongo import MongoClient
from pprint import pprint

client = MongoClient()
database = client.myDB
collection = database.movies

# A. Update all movies with "NOT RATED" at the "rated" key to be "Pending rating". The operation must be in-place and atomic.
collection.update_many(
    {"rated": "NOT RATED"}, 
    {"$set": {"rated": "Pending rating"}})


# B. Find a movie with your genre in imdb and insert it into your database with the fields listed in the hw description.
collection.insert_one(
    {"title": "Blockers", 
    "year": 2018, 
    "countries": ["USA"], 
    "genres": ["Comedy"], 
    "directors": ["Kay Cannon"], 
    "imdb": {"id": 2531344, "rating": 6.6, "votes": 6383}})


# C. Use the aggregation framework to find the total number of movies in your genre.
# Example result:
#  => [{"_id"=>"Comedy", "count"=>14046}]
result = collection.aggregate([
    {"$unwind": "$genres"}, 
    {"$match": {"genres": "Comedy"}}, 
    {"$group": {"_id": "$genres", "count": {"$sum": 1}}}
    ])
print("Part C")
print(list(result))
print()


# D. Use the aggregation framework to find the number of movies made in the country you were born in with a rating of "Pending rating".
# Example result when country is Hungary:
#  => [{"_id"=>{"country"=>"Hungary", "rating"=>"Pending rating"}, "count"=>9}]
result = collection.aggregate([
    {"$unwind": "$countries"}, 
    {"$match": {"countries": "Hungary", "rated": "Pending rating"}}, 
    {"$group": {"_id": {"country": "$countries", "rated": "$rated"}, "count": {"$sum": 1}}}
    ])
print("Part D")
print(list(result))
print()


# E. Create an example using the $lookup pipeline operator. See hw description for more info.
database.orders.insert_one({"item": "apple", "quantity": 1})
database.orders.insert_one({"item": "orange", "quantity": 2})
database.inventory.insert_one({"item": "apple", "instock": 100})
database.inventory.insert_one({"item": "orange", "instock": 200})

result = database.orders.aggregate([
    {"$lookup": 
        {"from": "inventory", 
        "localField": "item", 
        "foreignField": "item", 
        "as": "inventory_info"}
    }])
print("Part E")
pprint(list(result))
