# Twitter big-data playground

I'm a data engineer, and was looking for a fun project to do at home, that would give me some more experience with AWS, Spark, and other data types.

# get_tweets.py

A simple python script that is designed to run in a t2.Micro EC2 instance (the Nano got a little laggy), and using the standard Twitter search API, grabs the tweets you specify.  I have it set up to download every tweet made to (typically in reply) a very prolific Twitter user, downloading between 9 and 20+ gigabytes a day.

At last count, I've downloaded soe 350 million tweets.

# clean_tweets.py

An unfinished PySpark script, developed locally using Spark Standalone, but run using EMR, that cleans up the tweet objects, then cleans the text to be fed into another set of scripts that will do some topic and sentiment analysis.  

The outputs will be
1. A file of tweets, with columns    
    * id_str
    * user_id_str
    * in_reply_to_status_id
    * quoted_status_id_str
    * geo.coordinates
    * clean text as string
    * clean text as tokens array 
1. A file of hashtags, columns for id_str and the hashtag
1. A file of urls, columns for id_str and the url
1. A file of user mentions, columns for id_str and the mentioned user
1. A file of emojis, columns for id_str and the emoji

I plan to use the emojis and hashtags to do some improvements for sentiment.