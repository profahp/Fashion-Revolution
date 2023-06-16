import pandas as pd
import re
import json
import csv

# Cataglog of how tweet csv will be grouped
data_catalog = {
    "grp_13-15": ["2013FashionRevdata.csv", "2014FashionRevdata.csv", "2015FashionRevdata.csv"],
    "grp_16-19": ["2016FashionRevdata.csv", "2017FashionRevdata.csv", "2018FashionRevdata.csv",
                  "2019FashionRevdata.csv"],
    "grp_20-22": ["2020FashionRevdata.csv", "2021FashionRevdata.csv", "2022FashionRevdata.csv"]
}

RETWEET = "retweet"
REPLY = "reply"
QUOTE_TWEET = "quote_tweet"
MENTION = "mention"

DATA_DIR = "../data_dir/"


def group_csv():
    # loop through catalog, convert csv list for each group to a pandas data frame
    # Merge the data frames
    # send dataframe to clean_data_frame func
    # save new csv.

    for key, value in data_catalog.items():
        df_list = [DATA_DIR + x for x in value]
        df = pd.concat(map(pd.read_csv, df_list), ignore_index=True)
        df = clean_data_frame(df)
        df.to_csv(DATA_DIR + key + ".csv", encoding='utf-8', index=False)


# this is a faster way to clean the data frame
def clean_data_frame(data_frame):
    # Filter only english language data
    cleaned_df = data_frame[data_frame.lang == "en"]

    # using following columns
    cleaned_df = cleaned_df[
        ['id', 'referenced_tweets.replied_to.id', 'referenced_tweets.retweeted.id', 'referenced_tweets.quoted.id',
         'in_reply_to_user_id', 'in_reply_to_username', 'retweeted_user_id', 'retweeted_username', 'quoted_user_id',
         'quoted_username', 'created_at', 'text', 'public_metrics.impression_count', 'public_metrics.reply_count',
         'public_metrics.retweet_count', 'public_metrics.quote_count',
         'public_metrics.like_count', 'entities.annotations', 'entities.hashtags', 'entities.mentions', 'entities.urls',
         'author.id', 'author.created_at', 'author.username', 'author.name',
         'author.description', 'author.entities.description.hashtags', 'author.entities.description.mentions',
         'author.entities.description.urls', 'author.entities.url.urls',
         'author.url', 'author.public_metrics.followers_count', 'author.public_metrics.following_count',
         'author.public_metrics.listed_count', 'author.public_metrics.tweet_count']]
    '''cleaned_df['referenced_tweets.replied_to.id'] = cleaned_df['referenced_tweets.replied_to.id'].apply(
        lambda x: '{}'.format(x))
    cleaned_df['referenced_tweets.retweeted.id'] = cleaned_df['referenced_tweets.retweeted.id'].apply(
        lambda x: '{}'.format(x))'''

    # New column tweet_type added
    cleaned_df['retweeted_username'] = cleaned_df.retweeted_username.astype(str)
    cleaned_df['in_reply_to_username'] = cleaned_df.in_reply_to_username.astype(str)
    cleaned_df['quoted_username'] = cleaned_df.quoted_username.astype(str)
    cleaned_df['author.username'] = cleaned_df['author.username'].astype(str)
    cleaned_df['tweet_type'] = cleaned_df.apply(get_tweet_type, axis=1)
    cleaned_df['tweet_text_cleaned'] = cleaned_df['text'].apply(clean_tweet_text)
    cleaned_df['clean_text_adv'] = cleaned_df['text'].apply(clean_tweet_text_advanced)
    return cleaned_df


# determining tweet type
def get_tweet_type(row):
    if not pd.isnull(row['retweeted_user_id']):
        return RETWEET
    elif not pd.isnull(row['in_reply_to_user_id']):
        return (REPLY)
    elif not pd.isnull(row['quoted_user_id']):
        return QUOTE_TWEET
    else:
        return "None"


# Define the cleaning function
def clean_tweet_text(tweet):
    # Remove URLs
    tweet = str(tweet)
    # remove new lines
    tweet = " ".join(tweet.split("\n"))
    # tweet = re.sub(r'http\S+', '', tweet)
    # Remove mentions
    # tweet = re.sub(r'@\w+', '', tweet)
    # Remove hashtags
    # tweet = re.sub(r'#\w+', '', tweet)
    # Remove non-alphanumeric characters
    link_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    tweet = re.sub(link_pattern, '', tweet)
    # removes non alphanumeric chars ig
    tweet = re.sub(r'[^\sa-zA-Z0-9-@#]', '', tweet)
    tweet = tweet.replace("\n", ' ')
    # Convert to lowercase
    tweet = tweet.lower().strip()
    # Remove extra spaces
    # tweet = re.sub(r'\s+', ' ', tweet)
    return tweet


# This module removes # & @ from tweet-text
def clean_tweet_text_advanced(tweet):
    # remove new lines
    tweet = " ".join(tweet.split("\n"))
    # remove urls
    link_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    tweet = re.sub(link_pattern, '', tweet)
    # removes non-alphanumeric chars
    tweet = re.sub(r'[^\sa-zA-Z0-9-]', '', tweet)
    # Convert to lowercase remove white spaces
    tweet = tweet.lower().strip()
    return tweet


def get_mentions(obj):
    return '' if obj == '' else [x.replace("@", "") for x in json.loads(str(obj))]


def get_target(row):
    target = None
    if row['tweet_type'] == REPLY:
        target = row['in_reply_to_username']
    elif row['tweet_type'] == RETWEET:
        target = row['retweeted_username']
    elif row['tweet_type'] == QUOTE_TWEET:
        target = row['quoted_username']
    else:
        target = row['author.username']
    if not target or target == 'nan':
        target = row['author.username']
    return target


# generating the edge list
def gen_edge_list_df(df):
    edge_df = pd.DataFrame()
    edge_df['Time'] = df['created_at']
    edge_df['Source'] = df['author.username']
    edge_df['Target'] = df.apply(get_target, axis=1)
    edge_df['Type'] = df['tweet_type']
    edge_df['Tweet'] = df['tweet_text_cleaned']
    edge_df['TweetID'] = df['id']
    edge_df['Author_Bio'] = df['author.description']

    # df['clean_bio'] = df['author.description'].apply(clean_tweet_text)
    # df_edge = df.apply(process_row, axis=1tweet_type)

    # Create new columns in the dataframe for mentions
    # df['author.entities.description.mentions'].fillna('', inplace=True)
    # mentions = df['author.entities.description.mentions'].apply(get_mentions)
    mention_list = []
    for index, row in df.iterrows():
        if not pd.isnull(row['author.entities.description.mentions']):
            mentions = get_mentions(row['author.entities.description.mentions'])
            for m in mentions:
                mention_list.append(
                    {'Time': row['created_at'],
                     'Source': row['author.username'],
                     'Target': m,
                     'Author_Bio': row['author.description'],
                     'Type': MENTION,
                     'Tweet': row['tweet_text_cleaned'],
                     'TweetID': row['id']})
    df_mentions = pd.DataFrame(mention_list)
    final_df = pd.concat([edge_df, df_mentions])

    return final_df
    # Remove rows with missing values
    # edge_list_file = csv_file.replace(".csv", "_edge_list_bio.csv")
    # final_df.to_csv(edge_list_file, encoding='utf-8', index=False)


# generating a file for actos and bios only
def create_actor_bio_csv(file):
    df = pd.read_csv(file, index_col=0)
    actor_bio_df = df[['author.username', 'author.description']]
    actor_bio_df = actor_bio_df.drop_duplicates()
    new_file = file.replace(".csv", "_actor_bio.csv")
    actor_bio_df.to_csv(new_file, encoding='utf-8', index=False)


def actor_bio_groups():
    create_actor_bio_csv(DATA_DIR + "grp_13-15.csv")
    create_actor_bio_csv(DATA_DIR + "grp_16-19.csv")
    create_actor_bio_csv(DATA_DIR + "grp_20-22.csv")


# generating an edge list that includes bios
def generate_edge_with_bio_groups():
    #    optimize_edge_list_gen(DATA_DIR + "grp_13-15.csv")
    #   optimize_edge_list_gen(DATA_DIR + "grp_16-19.csv")
    #    optimize_edge_list_gen(DATA_DIR + "grp_20-22.csv")
    pass


# if you have a raw csv, this will generate an edge list
def parse_raw_csv(raw_csv):
    df = pd.read_csv(raw_csv)
    # DO All Data Cleaning
    df = clean_data_frame(df)
    # save a cleaned dataframe
    df.to_csv(raw_csv.replace(".csv", "_cleaned.csv"), encoding='utf-8', index=False)
    edge_list_df = gen_edge_list_df(df)
    edge_list_df.to_csv(raw_csv.replace(".csv", "_edge_list.csv"), encoding='utf-8', index=False)


def get_top_retweet(df, range):
    df = df.sort_values(by='public_metrics.retweet_count', ascending=False)
    return df.nlargest(range, 'public_metrics.retweet_count')


def get_top_likes_tweet(df, range):
    df = df.sort_values(by='public_metrics.like_count', ascending=False)
    return df.nlargest(range, 'public_metrics.like_count')


def get_top_replied_tweet(df, range):
    df = df.sort_values(by='public_metrics.reply_count', ascending=False)
    return df.nlargest(range, 'public_metrics.reply_count')


# sample function to aggregate data and get top tweets
def consolidate_fash_rev_data():
    csv_files = ["/home/aamir/projects/fashion_revolution/data_dir/2013FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2014FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2015FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2016FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2017FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2018FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2019FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2020FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2021FashionRevdata.csv",
                 "/home/aamir/projects/fashion_revolution/data_dir/2022FashionRevdata.csv"]
    df = pd.concat(map(pd.read_csv, csv_files), ignore_index=True)

    df = df[
        ['id', 'referenced_tweets.replied_to.id', 'referenced_tweets.retweeted.id', 'referenced_tweets.quoted.id',
         'in_reply_to_user_id', 'in_reply_to_username', 'retweeted_user_id', 'retweeted_username', 'quoted_user_id',
         'quoted_username', 'created_at', 'text', 'public_metrics.impression_count', 'public_metrics.reply_count',
         'public_metrics.retweet_count', 'public_metrics.quote_count',
         'public_metrics.like_count', 'entities.annotations', 'entities.hashtags', 'entities.mentions',
         'entities.urls',
         'author.id', 'author.created_at', 'author.username', 'author.name',
         'author.description', 'author.entities.description.hashtags', 'author.entities.description.mentions',
         'author.entities.description.urls', 'author.entities.url.urls',
         'author.url', 'author.public_metrics.followers_count', 'author.public_metrics.following_count',
         'author.public_metrics.listed_count', 'author.public_metrics.tweet_count']]
    df['retweeted_username'] = df.retweeted_username.astype(str)
    df['in_reply_to_username'] = df.in_reply_to_username.astype(str)
    df['quoted_username'] = df.quoted_username.astype(str)
    df['author.username'] = df['author.username'].astype(str)
    df['public_metrics.reply_count'] = df['public_metrics.reply_count'].astype(int)
    df['public_metrics.like_count'] = df['public_metrics.like_count'].astype(int)
    df['public_metrics.retweet_count'] = df['public_metrics.retweet_count'].astype(int)
    df['tweet_text_cleaned'] = df['text'].apply(clean_tweet_text)
    df['tweet_url'] = df.apply(
        lambda row: "https://twitter.com/{0}/status/{1}".format(row['author.username'], row['id']), axis=1)
    df['tweet_type'] = df.apply(get_tweet_type, axis=1)
    df.to_csv("fash_rev_data.csv", index=False)


def get_top_actors_tweet(csv_file):
    df = pd.read_csv(csv_file)
    # remove retweets
    df = df[df['tweet_type'] != RETWEET]

    # retweets
    top_retweet_df = get_top_retweet(df, 20)
    condensed_df = top_retweet_df[['author.username', 'tweet_text_cleaned', 'clean_tweet_text_advanced', 'public_metrics.retweet_count']]
    condensed_df.to_csv("top_retweet_tweets.csv", index=False)

    # likes
    top_likes_df = get_top_likes_tweet(df, 20)
    condensed_df = top_likes_df[['author.username', 'tweet_text_cleaned', 'clean_tweet_text_advanced', 'public_metrics.like_count']]
    condensed_df.to_csv("top_likes_tweets.csv", index=False)

    # Replied
    top_replied_df = get_top_replied_tweet(df, 20)
    condensed_df = top_replied_df[['author.username', 'tweet_text_cleaned', 'clean_tweet_text_advanced', 'public_metrics.reply_count']]
    condensed_df.to_csv("top_replied_to.csv", index=False)


def top_actors_edge_list(cleaned_csv):
    df = pd.read_csv(cleaned_csv)

    non_r_df = df[df['tweet_type'] != RETWEET]
    non_r_df = non_r_df.sort_values(by='public_metrics.retweet_count', ascending=False)
    non_r_df = non_r_df.drop_duplicates(subset='author.username')
    top_retweets_df = get_top_retweet(non_r_df, 20)
    top20_authors = top_retweets_df['author.username'].to_list()
    filtered_df = df[df['retweeted_username'].isin(top20_authors)]
    edge_df = pd.DataFrame()
    edge_df['Time'] = filtered_df['created_at']
    edge_df['TweetID'] = filtered_df['id']
    edge_df['Source'] = filtered_df['author.username']
    edge_df['Target'] = filtered_df['retweeted_username']
    edge_df['Tweet'] = filtered_df['tweet_text_cleaned']
    edge_df.to_csv("top_retweeters_edge_list.csv", index=False)


# use this to call any main function. For example: create_actor_bio_csv(filename)
if __name__ == "__main__":
    # csv file should contain tweet type column
    get_top_actors_tweet("fash_rev_data.csv")
    # filename = "/home/aamir/projects/fashion_revolution/data_dir/PLM_tweets_May20_June5,2021.csv"
    # parse_raw_csv(filename)
