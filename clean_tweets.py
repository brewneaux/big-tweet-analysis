from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pickle

spark = SparkSession.builder.appName("Transform Tweets").getOrCreate()
sc = spark.sparkContext

schema_pickle = None  # replace in prod!
schema = pickle.loads(schema_pickle)

df = spark.read.json("s3://trump.tweet.replies/replies_*", schema=schema)


##############
## Build Text DF
##############

sub_df = df.where(df.truncated == True)

tweet_df = sub_df.select(sub_df.id_str, sub_df.in_reply_to_status_id_str, df.quoted_status_id_str, sub.coordinates.coordinates, sub_df.text)

##############
## HashTags
##############

ht = df.select(df.id_str, df.entities.hashtags.alias("hashtags"))
ht = ht.where(size(ht.hashtags) > 0)
ht_ex = ht.select(ht.id_str, explode(ht.hashtags).alias("hashtag"))
all_hashtags = ht_ex.select(
    ht_ex.id_str,
    ht_ex.hashtag.text.alias('text'),
    ht_ex.hashtag.indices.getItem(0).alias("start"),
    (ht_ex.hashtag.indices.getItem(1) - 1).alias("end"),
)

##############
## URLs
##############

urls = df.select(df.id_str, df.entities.urls.alias("urls"))
urls = urls.where(size(urls.urls) > 0)
urls_ext = urls.select(urls.id_str, explode(urls.urls).alias('url'))
all_urls = urls_ext.select(
    urls_ext.id_str,
    urls_ext.url.display_url.alias('display_url'),
    urls_ext.url.url.alias('url'),
    urls_ext.url.indices.getItem(0).alias("start"),
    (urls_ext.url.indices.getItem(1) - 1).alias("end"),
)


##############
## UserMentions
##############

user_mentions = df.select(df.id_str, df.entities.user_mentions.alias("user_mentions"))
user_mentions = user_mentions.where(size(user_mentions.user_mentions) > 0)
user_mentions_ext = user_mentions.select(user_mentions.id_str, explode(user_mentions.user_mentions).alias('user_mention'))
all_mentions = user_mentions_ext.select(
    user_mentions_ext.id_str,
    user_mentions_ext.user_mention.screen_name.alias('screen_name'),
    user_mentions_ext.user_mention.indices.getItem(0).alias("start"),
    (user_mentions_ext.user_mention.indices.getItem(1) - 1).alias("end"),
)



##############
## Prep the emojis
##############
# import requests
# emoji = requests.get('https://raw.githubusercontent.com/muan/emojilib/master/emojis.json')
# emoji_list = []
# for k,v in emojis.iteritems():
#     emoji_list.append({'name': k, 'char': v['char']})

emoji_json = [removed for brevity]
emoji_list = json.loads(emoji_json)
emoji_df = spark.createDataFrame(emoji_list, schema=StructType((StructField('name', StringType(), False), StructField('char', StringType(), False))))

##############
## Remove the above from the text
##############

hashtag_remove = all_hashtags.select(
    all_hashtags.id_str,
    concat(lit('#'), all_hashtags.text).alias('text')
).groupBy(col('id_str')).agg(concat_ws('|', collect_set(col('text'))).alias('pattern'))

url_remove = all_urls.select(
    all_urls.id_str,
    all_urls.url.alias('text')
).groupBy(col('id_str')).agg(concat_ws('|', collect_set(col('text'))).alias('pattern'))


mention_remove = all_mentions.select(
    all_mentions.id_str,
    concat(lit('@'), all_mentions.screen_name).alias('text')
).groupBy(col('id_str')).agg(concat_ws('|', collect_set(col('text'))).alias('pattern'))

hashtag_remove.createOrReplaceTempView('hashtag_remove')
url_remove.createOrReplaceTempView('url_remove')
mention_remove.createOrReplaceTempView('mention_remove')
tweet_df.createOrReplaceTempView('tweet_df')

tweet_df.join(hashtag_remove, tweet_df.id_str == hashtag_remove.id_str, how='left').seletExprt

clean_df = spark.sql('''
    SELECT t.*,
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(t.text, IF(hr.pattern IS NOT NULL, hr.pattern, '$^'), '~HASHTAG~'), 
        IF(ur.pattern IS NOT NULL, ur.pattern, '$^'), '~URL~'), 
    IF(mr.pattern IS NOT NULL, mr.pattern, '$^'), '~USERMENTION~') AS cleanText
    FROM
        tweet_df t
        LEFT JOIN hashtag_remove hr
            ON t.id_str = hr.id_str
        LEFT JOIN url_remove ur
            ON t.id_str = ur.id_str
        LEFT JOIN mention_remove mr
            ON t.id_str = mr.id_str
''')


##############
## Emojis
##############

# Get the emojis out of the text
cd = clean_df.crossJoin(emoji_df)
cd.where(cd.cleanText.contains(cd.char)).select(col('id_str'), col('name')).limit(100).toPandas()

all_emojis = '|'.join([x['char'] for x in emoji_list])
clean_df = clean_df.withColumn('cleanText', regexp_replace(clean_df.cleanText, all_emojis, '~EMOJI~'))



##############
## Final text cleaning
##############

# Clean up some weirdo characters
accents = 'ÀÁÂÃÄÅàáâãäåĀāĂăĄąÇçĆćĈĉĊċČčÐðĎďĐđÈÉÊËèéêëĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħÌÍÎÏìíîïĨĩĪīĬĭĮįİıĴĵĶķĸĹĺĻļĽľĿŀŁłÑñŃńŅņŇňŉŊŋÒÓÔÕÖØòóôõöøŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšſŢţŤťŦŧÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųŴŵÝýÿŶŷŸŹźŻżŽž'
unaccented = 'AAAAAAaaaaaaAaAaAaCcCcCcCcCcDdDdDdEEEEeeeeEeEeEeEeEeGgGgGgGgHhHhIIIIiiiiIiIiIiIiIiJjKkkLlLlLlLlLlNnNnNnNnnNnOOOOOOooooooOoOoOoRrRrRrSsSsSsSssTtTtTtUUUUuuuuUuUuUuUuUuUuWwYyyYyYZzZzZz'


clean_df = clean_df.where(clean_df.text.rlike(r'[{}]'.format(accents))).select('text', translate(col('text'), accents, unaccented))
clean_df = clean_df.withColumn('cleanText', regexp_replace(clean_df.cleanText, '\\p{So}', '~EMOJI~'))

clean_df = clean_df.withColumn('cleanText', lower(clean_df.cleanText))

clean_df = clean_df.withColumn('cleanText', regexp_replace(clean_df.cleanText, '[^a-zA-Z0-9~]', ''))

clean_df = clean_df.withColumn('cleanText', regexp_replace(clean_df.cleanText, '\s+', ' '))


from pyspark.ml.feature import Tokenizer


tokenizer = Tokenizer(inputCol="cleanText", outputCol="cleanTextTokenized")
clean_df_tokenized = tokenizer.transform(clean_df)


