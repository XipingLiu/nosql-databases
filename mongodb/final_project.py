# Put the use case you chose here. Then justify your database choice:
#
# The use-case I chose is Hackernews, and the NoSQL database I chose is MongoDB.
# I chose MongoDB because it has flexible schema. I can incorporate votes and comments inside an article,
# while I can also choose to use seperate collections for votes and comments.
# In my implementation, I chose to use seperate collections for articles, votes, and comments, and used
# manual reference to build relationships between them.
# Another reason is that MongoDB has aggregation, which helps me to do queries easily.
# I can easily make queries such as "count how many up-votes article1 has".
#
#
#
# Explain what will happen if coffee is spilled on one of the servers in your cluster, causing it to go down.
# 
# MongoDB supports replication. There are multiple replicas of the data.
# If the coffee is spilled on the primary server, then one of the secondary servers will be elected 
# as the new primary server, and the database can continue to work.
#
#
#
# What data is it not ok to lose in your app? What can you do in your commands to mitigate the risk of lost data?
# 
# My database mainly have 4 collections: users, articles, comments, and votes.
# I think all of them are important and users don't want those information to lose.
# MongoDB supports write concern. When some data is written to the database, it will apply to not only
# the primary server but also several replicas before the response is sent back.
# I can use "Write Concern Specification" in my commands to mitigate the risk.
#

import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.hackernews

# Part 4: Create database objects
db.users.insert_many([
    {"_id": "user1",
     "name": "James",
     "email": "james@columbia.edu",
     "password": "iamjames"},
    {"_id": "user2",
     "name": "John",
     "email": "john@columbia.edu",
     "password": "iamjohn"},
    {"_id": "user3",
     "name": "Robert",
     "email": "robert@columbia.edu",
     "password": "iamrobert"},
    {"_id": "user4",
     "name": "Mary",
     "email": "mary@columbia.edu",
     "password": "iammary"},
    {"_id": "user5",
     "name": "Susan",
     "email": "susan@columbia.edu",
     "password": "iamsusan"}])

db.articles.insert_many([
    {"_id": "article1",
     "user_id": "user1",
     "title": "Who controls glibc?",
     "link": "https://lwn.net/SubscriberLink/753646/a6ebb50040c5862c/"},
    {"_id": "article2",
     "user_id": "user2",
     "title": "List of screw drives",
     "link": "https://en.wikipedia.org/wiki/List_of_screw_drives"},
    {"_id": "article3",
     "user_id": "user3",
     "title": "Examples of Growth Hacking",
     "link": "https://news.ycombinator.com/"},
    {"_id": "article4",
     "user_id": "user4",
     "title": "The oldest song in the world",
     "link": "http://www.bbc.com/travel/story/20180424-did-syria-create-the-worlds-first-song"},
    {"_id": "article5",
     "user_id": "user5",
     "title": "Operation Ivy Bells",
     "link": "https://en.wikipedia.org/wiki/Operation_Ivy_Bells"}])

db.votes.insert_many([
    {"article_id": "article1",
     "user_id": "user1",
     "type": "up_vote"},
    {"article_id": "article1",
     "user_id": "user2",
     "type": "up_vote"},
    {"article_id": "article1",
     "user_id": "user3",
     "type": "down_vote"},
    {"article_id": "article1",
     "user_id": "user4",
     "type": "down_vote"},
    {"article_id": "article1",
     "user_id": "user5",
     "type": "up_vote"}])

db.comments.insert_many([
    {"article_id": "article1",
     "user_id": "user2",
     "content": "We have our own monitoring stack, though, so don't use the additional prometheus integrations"},
    {"article_id": "article2",
     "user_id": "user3",
     "content": "I feel like this one is really on you, not Red Hat."},
    {"article_id": "article3",
     "user_id": "user4",
     "content": "It's literally the C library reference manual. That's true of the whole thing!"}])


# Part 5: Mutate your data
# Action 1: User "Susan" publishes an article
db.articles.insert_one(
    {"_id": "article6",
     "user_id": "user5",
     "title": "Zero day detection software",
     "link": "https://planetzuda.com/detecting-zero-days-in-software-automatically/2018/05/07/"})

# Action 2: User "Mary" comments on "Zero day detection software"
db.comments.insert_one(
    {"article_id": "article6",
     "user_id": "user4",
     "content": "I'm intrigued. Any video of this method?"})

# Action 3: User "Robert" deletes all his comments
db.comments.delete_many({"user_id": "user3"})

# Action 4: User "Robert" deletes all his votes
db.votes.delete_many({"user_id": "user3"})

# Action 5: User "Mary" updates her vote for "article1" from "down_vote" to "up_vote"
db.votes.update_one(
    {"user_id": "user4", "article_id": "article1"},
    {"$set": {"type": "up_vote"}})

# Action 6: User "John" updates his comment to "article1"
db.comments.update_one(
    {"user_id": "user2", "article_id": "article1"},
    {"$set": {"content": "I love this article!"}})

# Action 7: Return the link of the article called "Who controls glibc?"
result = db.articles.find(
    {"title": "Who controls glibc?"},
    {"title": 1, "link": 1, "_id": 0})
print(list(result))

# Action 8: Count how many up-votes for "article1"
result = db.votes.aggregate([
    {"$match": {"article_id": "article1"}},
    {"$group": {"_id": "$type", "count": {"$sum": 1}}}
    ])
print(list(result))
