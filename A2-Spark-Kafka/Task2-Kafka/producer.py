import sys
from kafka import KafkaProducer

# Get topic names from command line arguments
topic_likes = sys.argv[1]
topic_shares = sys.argv[2]
topic_comments = sys.argv[3]

# Initialize Kafka producers
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#producer_shares = KafkaProducer(bootstrap_servers=['localhost:9092'])
#producer_comments= KafkaProducer(bootstrap_servers=['localhost:9092'])

# Read input from standard input
for line in sys.stdin:
    
    if line == "EOF" or line == "eof" or line == "EOF\n" or line == "eof\n":
        producer.send(topic_likes, value='EOF'.encode('utf-8'))
        producer.send(topic_shares, value='EOF'.encode('utf-8'))
        producer.send(topic_comments, value='EOF'.encode('utf-8'))
        break
    line = line.strip()
    producer.send(topic_likes, value=line.encode('utf-8'))
    producer.send(topic_shares, value=line.encode('utf-8'))
    producer.send(topic_comments, value=line.encode('utf-8'))

# Close the producers
producer.flush()
producer.close()
#producer_shares.close()
#producer_comments.close()