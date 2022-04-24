import nltk
import pandas as pd
import re
import plotly.graph_objs as go
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# This is a Sentiment Analysis by using existed csv.file
# you have to set your own word_dictionary to help deal with data

# set Sentiment dictory
word_dict = {'manipulate': -1, 'manipulative': -1, 'jamescharlesiscancelled': -1, 'jamescharlesisoverparty': -1,
             'pedophile': -1, 'pedo': -1, 'cancel': -1, 'cancelled': -1, 'cancel culture': 0.4, 'teamtati': -1,
             'teamjames': 1,
             'teamjamescharles': 1, 'liar': -1}

# twitter data cleaner

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()
sid.lexicon.update(word_dict)
nltk.download('words')
words = set(nltk.corpus.words.words())

# put your csv file
df = pd.read_csv('raw_malaysia_politic_tweets.csv')

# it will return you a number, Closer to -1, more negative, closer to 1，more positive
sentence = df['text'][0]
print(sentence)
print(df['text'][10])
print(sid.polarity_scores(sentence)['compound'])

# a filter for data cleaning
def cleaner(tweet):
    tweet = re.sub("@[A-Za-z0-9]+", "", tweet)  # Remove @ sign
    tweet = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", tweet)  # Remove http links
    tweet = " ".join(tweet.split())
    tweet = tweet.replace("#", "").replace("_", " ")  # Remove hashtag sign but keep the text
    tweet = " ".join(w for w in nltk.wordpunct_tokenize(tweet)
                     if w.lower() in words or not w.isalpha())
    return tweet

# deploying the filter
df['tweet_clean'] = df['text'].apply(cleaner)

list1 = []
for i in df['tweet_clean']:
    list1.append((sid.polarity_scores(str(i)))['compound'])

df['sentiment'] = pd.Series(list1)


def sentiment_category(sentiment):
    label = ''
    if sentiment > 0:
        label = 'positive'
    elif sentiment == 0:
        label = 'neutral'
    else:
        label = 'negative'
    return label


df['sentiment_category'] = df['sentiment'].apply(sentiment_category)
df = df[['text', 'id', 'created_at','sentiment_category']]
print(df.head())

# Sets what properties to use as criteria for line charts
neg = df[df['sentiment_category']=='negative']
neg = neg.groupby(['created_at'],as_index=False).count()
pos = df[df['sentiment_category']=='positive']
pos = pos.groupby(['created_at'],as_index=False).count()
pos = pos[['created_at','id']]
neg = neg[['created_at','id']]

# draw line graph
fig = go.Figure()
for col in pos.columns:
    fig.add_trace(go.Scatter(x=pos['created_at'], y=pos['id'],
                             name = col,
                             mode = 'markers+lines',
                             line=dict(shape='linear'),
                             connectgaps=True,
                             line_color='green'
                             )
                 )
for col in neg.columns:
    fig.add_trace(go.Scatter(x=neg['created_at'], y=neg['id'],
                             name = col,
                             mode = 'markers+lines',
                             line=dict(shape='linear'),
                             connectgaps=True,
                             line_color='red'
                             )
                 )
fig.show()