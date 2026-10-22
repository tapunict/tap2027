import os
from atproto import Client
import socket
import json
from time import sleep
from datetime import datetime

# Credits https://github.com/sspaeti/bsky-atproto/blob/main/python/streaming_into_motherduckdb.py

def fetchPost():
    global bsky_client
    global hashtag
    global s
    global logstash_host
    global logstash_port
    global last_fetched_timestamp
    
    cursor = None
    new_posts_found = False
    
    while True:
        print (f"Fetching posts with hashtag {hashtag}")
        
        # Build search parameters
        search_params = {'q': f'#{hashtag}', 'cursor': cursor, 'limit': 25}
        if last_fetched_timestamp:
            search_params['since'] = last_fetched_timestamp
            print(f"Searching for posts since {last_fetched_timestamp}")
        
        fetched = bsky_client.app.bsky.feed.search_posts(params=search_params)
        for post in fetched.posts:
            post_timestamp = post.record.created_at
            
            # Skip posts older than our last fetch
            if last_fetched_timestamp and post_timestamp <= last_fetched_timestamp:
                print(f"Skipping old post from {post_timestamp}")
                continue
            
            new_posts_found = True
            
            # Safely handle langs attribute
            langs = None
            if hasattr(post.record, 'langs') and post.record.langs:
                try:
                    langs = ','.join(post.record.langs)
                except:
                    langs = None
            post_data = {
                        'uri': post.uri,
                        'cid': post.cid,
                        'author': post.author.handle,
                        'text': post.record.text,
                        'created_at': post.record.created_at,
                        'hashtag': hashtag,
                        'langs': langs
                    }
            
            match logger:
                case "stdout":
                    print(post_data)
                case "logstash_tcp":
                    try:
                        message = json.dumps(post_data) + '\n'
                        s.sendall(message.encode('utf-8'))
                        print(f"Sent post to Logstash: {post.uri}")
                    except (BrokenPipeError, ConnectionResetError, OSError) as e:
                        print(f"Connection error: {e}. Reconnecting...")
                        try:
                            s.close()
                        except:
                            pass
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((logstash_host, logstash_port))
                        message = json.dumps(post_data) + '\n'
                        s.sendall(message.encode('utf-8'))
                        print(f"Reconnected and sent post to Logstash: {post.uri}")
    
        if not fetched.cursor:
            break            
        cursor = fetched.cursor
        print(f"Next cursor: {cursor}")
    
    # Update timestamp after successful fetch
    if new_posts_found or last_fetched_timestamp is None:
        last_fetched_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        print(f"Updated last fetch timestamp to: {last_fetched_timestamp}")

def initializeLogger():
    global s
    match logger:
        case "stdout":
            print("Logger: stdout")
        case "logstash_tcp": 
            print(f"Logger: logstash_tcp at {logstash_host}:{logstash_port}")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((logstash_host, logstash_port))
            except Exception as e:
                print(f"Error connecting to Logstash: {e}")
                # TODO: Handle error

def finalizeLogger():
    global s
    match logger:
        case "logstash_tcp":
            s.close()
            print("Closed connection to Logstash")

print("*** BlueSky connector v 1.0 ***")

app_password = os.getenv("BLUESKY_APP_PASSWORD") # TODO: Move to secret
handle = os.getenv("BLUESKY_HANDLE")
logger = os.getenv("BLUESKY_LOGGER", "stdout")
hashtag = os.getenv("BLUESKY_HASHTAG", "tapunict")


# Define Logstash address and port
logstash_host = 'logstash'
logstash_port = 5000

# Create AT Proto Client
bsky_client = Client()

password = app_password
bsky_client.login(handle, password)
last_fetched_timestamp = None  # Track last successful fetch time

while True:
    print("Starting new search")
    initializeLogger()
    fetchPost()
    finalizeLogger()
    sleep(5)

