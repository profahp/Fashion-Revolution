import tweepy
import json
from pymongo import MongoClient

# assuming you have mongoDB installed locally
MONGO_HOST= ''

storage = MongoClient(MONGO_HOST)

# Use database
db = storage.political_data_analysis

client = tweepy.Client(bearer_token='')

# Replace with your own search query
query = 'Nintendo, Sony'

# Replace with start time period of your choice
start_time = '2010-01-01T00:00:00Z'

# Replace with end time period of your choice
end_time = '2020-08-01T00:00:00Z'

# Set tweet's fields
fields = ['author_id', 'source', 'context_annotations', 'created_at', 'geo', 'in_reply_to_user_id', ]

# this part due to twitter account limit, you could not get too much data, need to check account
for tweet in tweepy.Paginator(client.search_all_tweets, query=query, tweet_fields=fields, start_time=start_time, end_time=end_time, max_results=100).flatten(limit=100):
    
    # Use 'political_data_analysis' database, if it does not exist, will be created
    db = storage.political_data_analysis

    # Datatype from 'dic' to 'JSON' Object
    temp= json.dumps(tweet.data)

    # Decode the JSON 
    datajson = json.loads(temp)
 
    # grab the 'created_at' data from the Tweet to use for display
    created_at = datajson['created_at']

    # print out a message to the screen that we have collected a tweet
    print("Tweet collected at " + str(created_at))

    # insert the data into the mongoDB into a collection, if the collection does not exist, will be created
    db.test_tweets2.insert_one(datajson)

print(client.rate_limit_status()["resources"])