from kafka import KafkaConsumer
import json
import sys

# Get topic name from command line argument
topic_comments = sys.argv[3]
topic_likes = sys.argv[1]
# Initialize Kafka consumer
consumer = KafkaConsumer(topic_comments, bootstrap_servers=['localhost:9092'])
# Initialize a dictionary to store comments for each user
comments_dict = {}

# Iterate through the messages
for message in consumer:

    activity = message.value.decode('utf-8')
    if activity == "EOF":
        break
    # Split the activity into its components
    components = activity.split(' ')
    if components[0]!="comment":
        continue
    user = components[2]
    comment = ' '.join(components[4:])
    comment = comment.strip('"')
    # Check if the user is already in the comments_dict
    if user not in comments_dict:
        comments_dict[user] = []
    
    # Add the comment to the user's list of comments
    comments_dict[user].append(comment)

# Print the comments_dict in the desired format
# sort the comments_dict based on the key and then print using json.dumps
print(json.dumps(comments_dict, indent=4, sort_keys=True))

