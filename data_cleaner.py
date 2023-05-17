import pandas as pd
import re
import json
import csv
from progress.bar import Bar

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
    cleaned_df['tweet_type'] = cleaned_df.apply(get_tweet_type, axis=1)
    cleaned_df['tweet_text_cleaned'] = cleaned_df['text'].apply(clean_tweet_text)
    return cleaned_df


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
    tweet = re.sub(r'http\S+', '', tweet)
    # Remove mentions
    #tweet = re.sub(r'@\w+', '', tweet)
    # Remove hashtags
    #tweet = re.sub(r'#\w+', '', tweet)
    # Remove non-alphanumeric characters
    tweet = re.sub(r'\W+', ' ', tweet) #Todo ignore '@' remove rest
    # Convert to lowercase
    tweet = tweet.lower()
    # Remove extra spaces
    tweet = re.sub(r'\s+', ' ', tweet).strip()
    return tweet


def get_mentions(obj):
    return '' if obj == '' else [x.replace("@", "") for x in json.loads(str(obj))]


def get_target(row):
    if row['tweet_type'] == REPLY:
        return row['in_reply_to_username']
    elif row['tweet_type'] == RETWEET:
        return row['retweeted_username']
    elif row['tweet_type'] == QUOTE_TWEET:
        return row['quoted_username']
    else:
        return row['author.username']


def gen_edge_list_df(df):
    edge_df = pd.DataFrame(columns=['Time', 'Source', 'Author_Bio', 'Type', 'Tweet', 'Label'])
    edge_df['Time'] = df['created_at']
    edge_df['Source'] = df['author.username']
    edge_df['Type'] = df['tweet_type']
    edge_df['Tweet'] = df['tweet_text_cleaned']
    edge_df['Label'] = df['id']
    edge_df['Target'] = df.apply(get_target, axis=1)
    edge_df['Author_Bio'] = df['author.description']

    # df['clean_bio'] = df['author.description'].apply(clean_tweet_text)
    # df_edge = df.apply(process_row, axis=1tweet_type)

    # Create new columns in the dataframe for mentions
    # df['author.entities.description.mentions'].fillna('', inplace=True)
    # mentions = df['author.entities.description.mentions'].apply(get_mentions)
    mention_list = []
    with Bar('Processing...') as bar:
        for index, row in df.iterrows():
            if not pd.isnull(row['author.entities.description.mentions']):
                mentions = get_mentions(row['author.entities.description.mentions'])
                for m in mentions:
                    mention_list.append(
                        {'Source': row['author.username'],
                         'Target': m,
                         'Author_Bio': row['author.description'],
                         'Type': MENTION,
                         'Tweet': row['tweet_text_cleaned'],
                         'Label': row['id']})
        bar.next()
    df_mentions = pd.DataFrame(mention_list)
    final_df = pd.concat([edge_df, df_mentions])

    return final_df
    # Remove rows with missing values
    # edge_list_file = csv_file.replace(".csv", "_edge_list_bio.csv")
    # final_df.to_csv(edge_list_file, encoding='utf-8', index=False)


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


def generate_edge_with_bio_groups():
    #    optimize_edge_list_gen(DATA_DIR + "grp_13-15.csv")
    #   optimize_edge_list_gen(DATA_DIR + "grp_16-19.csv")
    #    optimize_edge_list_gen(DATA_DIR + "grp_20-22.csv")
    pass


def parse_raw_csv(raw_csv):
    df = pd.read_csv(raw_csv)
    # DO All Data Cleaning
    df = clean_data_frame(df)
    # save a cleaned dataframe
    df.to_csv(raw_csv.replace(".csv", "_cleaned.csv"), encoding='utf-8', index=False)
    edge_list_df = gen_edge_list_df(df)
    edge_list_df.to_csv(raw_csv.replace(".csv", "_edge_list.csv"), encoding='utf-8', index=False)


if __name__ == "__main__":
    filename = "/home/aamir/projects/fashion_revolution/data_dir/2013FashionRevdata.csv"
    parse_raw_csv(filename)
