#Base and Cleaning
import pandas as pd

#Read the csv file into dataframe
file_path = r'C:/Users/Adrienne/Desktop/Amina Laddaoui Undergraduate Research Assistant 22-23/BLM-PLM Research/PLM_tweets_May20_June5,2021_cleaned.csv'
df = pd.read_csv(file_path)

#Sentiment Analysis
#make sure you have installed textblob in the terminal first: pip install textblob
from textblob import TextBlob

#Get the sentiments for each tweet
sentiment = []
for i in range(len(df)):
    text = df.loc[i, "tweet_text_cleaned"]
    if isinstance(text, float):
        sentiment.append(0)
    else:
        sentiment.append(TextBlob(str(text)).sentiment.polarity)

   
#Get the subjectivity of each tweet
subjectivity=[]
for i in range(len(df)):
    subjectivity.append(TextBlob(str(df.loc[i, "tweet_text_cleaned"])).sentiment.subjectivity)

df["sentiment"]=sentiment
df["subjectivity"]=subjectivity

sentimenttotal=0.0
for i in range(len(df)):
   sentimenttotal+=df.loc[i,"sentiment"]

subjectivitytotal=0.0
for i in range(len(df)):
   subjectivitytotal+=df.loc[i,"subjectivity"]

#Tweets with zero engagement
count=0
for i in range(len(df)):
  if(df.loc[i,'public_metrics.reply_count']==0 & df.loc[i,'public_metrics.like_count']==0):
    count+=1
#total tweets
print(len(df))
# no engagement tweet
print(count)
#percent no engagement
print(count/len(df))

sentimentavg= sentimenttotal/len(df)
subjectivityavg=subjectivitytotal/len(df)
print("sentiment avg:",sentimentavg)
print("subjectivity:",subjectivityavg)

liketotal=0.0
for i in range(len(df)):
   liketotal+=df.loc[i,"public_metrics.like_count"]

replytotal=0.0
for i in range(len(df)):
   replytotal+=df.loc[i,"public_metrics.reply_count"]

retweettotal=0.0
for i in range(len(df)):
   retweettotal+=df.loc[i,"public_metrics.retweet_count"]
   
quotetotal=0.0
for i in range(len(df)):
   quotetotal+=df.loc[i,"public_metrics.quote_count"]
   
likeavg= liketotal/len(df)
replyavg=replytotal/len(df)
retweetavg=retweettotal/len(df)
quoteavg=quotetotal/len(df)
print("like avg:",likeavg)
print("reply avg:",replyavg)
print("retweet avg:",retweetavg)
print("quote avg:",quoteavg)

#remove outliers for likes
pd_series_likes = df['public_metrics.like_count']
pd_series_adjusted = pd_series_likes[pd_series_likes.between(pd_series_likes.quantile(.01), pd_series_likes.quantile(.99))]
df['public_metrics.like_count_no_outliers']=pd_series_adjusted

#remove outliers for replies
pd_series_reply = df['public_metrics.reply_count']
pd_series_adjusted = pd_series_reply[pd_series_reply.between(pd_series_reply.quantile(.01), pd_series_likes.quantile(.99))]
df['public_metrics.reply_count_no_outliers']=pd_series_adjusted

df.plot(x='sentiment', y='public_metrics.like_count',style='o',figsize=(20,10))
df.plot(x='sentiment', y='public_metrics.reply_count',style='o',figsize=(20,10))
df.plot(x='sentiment', y='public_metrics.like_count_no_outliers',style='o',figsize=(20,10))
df.plot(x='sentiment', y='public_metrics.reply_count_no_outliers',style='o',figsize=(20,10))

#number of posts with positive sentiment
print((df["sentiment"]>0).sum())
#percent
print((df["sentiment"]>0).sum()/len(df))

#number of posts with neutral sentiment
print((df["sentiment"]==0).sum())
#percent
print((df["sentiment"]==0).sum()/len(df))

#number of posts with negative sentiment
print((df["sentiment"]<0).sum())
#percent
print((df["sentiment"]<0).sum()/len(df))


df["hashtags"]=df.text.str.lower().str.findall(r'#.*?(?=\s|$)')
print(df["hashtags"])
#method to create a hashtag dictionary
def to_1D(series):
 return pd.Series([x for _list in series for x in _list])
a=to_1D(df["hashtags"]).value_counts().to_string()[:3000]
print(a)
