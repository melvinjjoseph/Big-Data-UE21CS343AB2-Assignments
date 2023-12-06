from kafka import KafkaConsumer
import json
import sys

# Get topic name from command line argument
topic_likes = sys.argv[1]

# Initialize Kafka consumer
consumer = KafkaConsumer(topic_likes, bootstrap_servers=['localhost:9092'])

# Initialize a dictionary to store likes count for each user and post
likes_dict = {}

# Iterate through the messages
for message in consumer:
    # Decode the message value (assuming it's in utf-8)
    activity = message.value.decode()
    if activity == "EOF":
        break
    # Split the activity into its components
    components = activity.split(' ')
    if components[0]!="like":
        continue
    user = components[2]
    post_id = components[3]
    # Check if the user is already in the likes_dict
    if user not in likes_dict:
        likes_dict[user] = {}
    
    # Check if the post_id is already in the user's likes_dict
    if post_id not in likes_dict[user]:
        likes_dict[user][post_id] = 0
    
    # Increment the like count for the post
    likes_dict[user][post_id] += 1

# Print the likes_dict in the desired format
print(json.dumps(likes_dict, indent=4, sort_keys=True))