import logging
import time
import praw
import pandas as pd
from praw.models import MoreComments


handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
for logger_name in ("praw", "prawcore"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

CLIENT_ID = '' # availalble at Social Media Analytics Lab/Documentation/Prof HP Info.docx
CLIENT_SECRET = ''
USER_AGENT = 'user2'

subreddit_name = 'LGBTQMentalHealth'
post_limit = 550 # per session 600 max requests post_limit should be less than < 600

# Initialize the Reddit API wrapper
reddit = praw.Reddit(client_id=CLIENT_ID,
                     client_secret=CLIENT_SECRET,
                     user_agent=USER_AGENT)

# Example: Fetch the top 10 hot posts from the Python subreddit
posts = []
comment_list = []

subreddit = reddit.subreddit(subreddit_name)

hot_posts = subreddit.hot(limit=post_limit)
for post in hot_posts:
    posts.append(
        [post.title, post.score, post.id, post.subreddit, post.url, post.num_comments, post.selftext, post.created])
    time.sleep(0.5)
    submission = reddit.submission(id=post.id)
    for comment in submission.comments:
        if isinstance(comment, MoreComments):
            continue
        comment_list.append([post.id, comment.author, comment.body])

# save posts in a csv
post_df = pd.DataFrame(posts,columns=['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'])
post_df.to_csv("{}.csv".format(subreddit_name))
# save comments in a csv
comments_df = pd.DataFrame(comment_list, columns=['post_id', 'comment_author', 'text'])
comments_df.to_csv("{}_comments.csv".format(subreddit_name))
