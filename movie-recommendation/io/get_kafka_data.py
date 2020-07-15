# !/usr/bin/env python3

"""
Write from Kafka stream to ratings and views files
"""

from kafka import KafkaConsumer, TopicPartition
import pandas as pd

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: x.decode('utf-8')
)

topic_part = TopicPartition('movielog3', 0)
consumer.assign([topic_part])
end_offset = consumer.end_offsets([topic_part])[topic_part]
consumer.seek_to_beginning(topic_part)

raw_ratings = open('kafka_raw_ratings.csv', 'w')
raw_ratings.write(f"time,userid,movieid,rating\n")

raw_views = open('kafka_raw_views.csv', 'w')
raw_views.write("time,userid,movieid\n")

iterate = True
while iterate:
    response = consumer.poll(timeout_ms=5000)
    for message in response[topic_part]:
        time, userid, request = message.value.split(',')
        request_parts = request.split('/')
        # Consume rating
        if request_parts[1] == 'rate':
            movieid, rating = request_parts[2].split('=')
            raw_ratings.write(f"{time},{userid},{movieid},{rating}\n")
        # Consume view
        else:
            # Only consume views of the first minute
            if request_parts[4] == '0.mpg':
                movieid = request_parts[3]
                raw_views.write(f"{time},{userid},{movieid}\n")
    pos = consumer.position(topic_part)
    if pos >= end_offset:
        iterate = False

consumer.close()
raw_ratings.close()
raw_views.close()