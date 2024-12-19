import praw
from kafka import KafkaProducer
import json
import time

# Reddit API configuration
reddit = praw.Reddit(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    user_agent="Reddit Streamer"
)

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_stream(subreddit_name, topic_name, keywords, limit=100):
    subreddit = reddit.subreddit(subreddit_name)
    for post in subreddit.search(" OR ".join(keywords), limit=limit):
        message = {
            "id": post.id,
            "title": post.title,
            "author": post.author.name if post.author else None,
            "score": post.score,
            "url": post.url,
            "num_comments": post.num_comments,
            "created_utc": post.created_utc
        }
        producer.send(topic_name, value=message)
        time.sleep(1)  # Avoid hitting API limits

if __name__ == "__main__":
    subreddits = {"worldnews": "reddit_worldnews", "politics": "reddit_politics"}
    keywords = ["Israel", "Palestine", "conflict", "peace"]

    for subreddit_name, topic_name in subreddits.items():
        fetch_and_stream(subreddit_name, topic_name, keywords)
