# Open Questions

## Truncated Tweets

It appears that some tweets are truncated.
grep 1014959313079676929 /home/brewneaux/Desktop/trumpsent/data/aws/replies | head -n1 | jq            


Apparently, I need to add '&tweet_mode=extended' to my URL per https://twittercommunity.com/t/retrieve-full-tweet-when-truncated-non-retweet/75542/2

DUH .truncated == True

Need to do an analysis to see how many of these it hppens to.  Probably need to look for '... https://t.co' near the end of a tweet, and make sure its not a retweet?  In my sample file, doing `cat replies | jq '.text' | egrep '\.\.\. https://t.co.*$' | wc -l` only yields like 288 out of 95096. Might just be ok to ignore them.

# Cleaning steps

## Move hastags out to a new array

Hashtags may contain important information.  Make a new array in the df that contains those hashtags.

The reply array should contain an element .entities.hashtags, which contains a list of elements.  The hashtags are pre-parsed so we dont have to do magic.

### Example:

```
@realDonaldTrump LOL...  another corrupt jagoff bites the dust.  #WhoIsNext #basta
```

```
...
    "hashtags": [
      {
        "indices": [
          65,
          75
        ],
        "text": "WhoIsNext"
      },
      {
        "indices": [
          76,
          82
        ],
        "text": "basta"
      }
    ],

```

## Tokenize URLs, Hashtags, mentions, emojis 

### URLs

URLs are also parsed out by twitter and stored in an element of the tweet reply.  If spark can do this efficiently, I should be able to use the strings contained as urls in the array to replace them out of the actual text. Apparanently, they can be in any number of places. So far, I've found:

.entities.urls
.entities.media.[].url


#### Example

```
{
    "text": "RT @beth95cooper: @realDonaldTrump  https://t.co/b6mGvxHuMq",
    "etities": {
        "media": [
            {
                "source_user_id": 860202343408828400,
                "source_status_id_str": "1013972427624341504",
                "expanded_url": "https://twitter.com/beth95cooper/status/1013972427624341504/photo/1",
                "display_url": "pic.twitter.com/b6mGvxHuMq",
                "url": "https://t.co/b6mGvxHuMq",
                "media_url_https": "https://pbs.twimg.com/media/DhJai1lXUAAv3up.jpg",
                "source_user_id_str": "860202343408828417",
                "source_status_id": 1013972427624341500,
                "id_str": "1013972420657631232",
                "sizes": {
                "small": {
                    "h": 676,
                    "resize": "fit",
                    "w": 680
                },
                "large": {
                    "h": 700,
                    "resize": "fit",
                    "w": 704
                },
                "medium": {
                    "h": 700,
                    "resize": "fit",
                    "w": 704
                },
                "thumb": {
                    "h": 150,
                    "resize": "crop",
                    "w": 150
                }
                },
                "indices": [
                36,
                59
                ],
                "type": "photo",
                "id": 1013972420657631200,
                "media_url": "http://pbs.twimg.com/media/DhJai1lXUAAv3up.jpg"
            }
        ]
    }

}
```

### Hashtags

As stated above, hashtags are in a magic array, so I can prob just find/replace them from there.

### Mentions

Like everything else so far, they live in .entities.user_mentions

### Emoji

This one...might be harder. I kind of want to keep the emoji-meaning.  Make a DF, do a join with a regexp_replace

https://raw.githubusercontent.com/muan/emojilib/master/emojis.json

# Actual text cleaning

So by now, we should have turned a lot of the weird stuff into regular text.  Go through some text cleaning...

* convert to ascii-ish
* lower
* contractions
* punctuation
* clean whitespace

## Convert utf to ascii
accents = 'ÀÁÂÃÄÅàáâãäåĀāĂăĄąÇçĆćĈĉĊċČčÐðĎďĐđÈÉÊËèéêëĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħÌÍÎÏìíîïĨĩĪīĬĭĮįİıĴĵĶķĸĹĺĻļĽľĿŀŁłÑñŃńŅņŇňŉŊŋÒÓÔÕÖØòóôõöøŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšſŢţŤťŦŧÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųŴŵÝýÿŶŷŸŹźŻżŽž'
unaccented = 'AAAAAAaaaaaaAaAaAaCcCcCcCcCcDdDdDdEEEEeeeeEeEeEeEeEeGgGgGgGgHhHhIIIIiiiiIiIiIiIiIiJjKkkLlLlLlLlLlNnNnNnNnnNnOOOOOOooooooOoOoOoRrRrRrSsSsSsSssTtTtTtUUUUuuuuUuUuUuUuUuUuWwYyyYyYZzZzZz'
translate(col('text'), accents, unaccented)



# Output Columns:

* id_str
* user_id_str
* in_reply_to_status_id
* geo.coordinates
* clean text as string
* clean text as tokens array 
* hashtags
