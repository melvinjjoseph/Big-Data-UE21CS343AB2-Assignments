from kafka import KafkaConsumer
import json
import sys

# Get topic names from command line arguments
topic_likes = sys.argv[1]
topic_shares = sys.argv[2]
topic_comments = sys.argv[3]

# Initialize Kafka consumers
consumer_likes = KafkaConsumer(topic_likes, bootstrap_servers=['localhost:9092'])
#consumer_shares = KafkaConsumer(topic_shares, bootstrap_servers=['localhost:9092'])
#consumer_comments = KafkaConsumer(topic_comments, bootstrap_servers=['localhost:9092'])

# Initialize a dictionary to store popularity scores for each user
popularity_dict = {}

# Helper function to calculate popularity
def calculate_popularity(likes, shares, comments):
    return (likes + 20*shares + 5*comments) / 1000

# Iterate through the likes messages
for message in consumer_likes:
    # Decode the message value (assuming it's in utf-8)
    activity = message.value.decode('utf-8')
    if activity == "EOF":
        break
    # Split the activity into its components
    components = activity.split(' ')
    user = components[2]
    
    # Check if the user is already in the popularity_dict
    if user not in popularity_dict:
        popularity_dict[user] = {'likes': 0, 'shares': 0, 'comments': 0}
    
    if components[0]=="like":
        popularity_dict[user]['likes'] += 1
    elif components[0]=="comment":
        popularity_dict[user]['comments'] += 1
    elif components[0]=="share":
        num_shares=len(components[4:])
        popularity_dict[user]['shares'] += num_shares
    


# Calculate popularity scores and print the result
popularity_result = {}
for user, counts in popularity_dict.items():
    likes = counts['likes']
    shares = counts['shares']
    comments = counts['comments']
    popularity_score = calculate_popularity(likes, shares, comments)
    popularity_result[user] = popularity_score

# Print the popularity_dict in the desired format
print(json.dumps(popularity_result, indent=4, sort_keys=True))