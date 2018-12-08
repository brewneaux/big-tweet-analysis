import os
import requests
import base64
import os
import logging
import json
import time

OUTPUT_LOCATION = '/home/ec2-user/data/reply_full_details_premium'
CREDENTIAL_FILE = 'twitter_creds.json'
START_ID_FILE = 'start_id'
BASE_QUERY = "to:twitter"


def get_logger():
    """Gets the default logger, registering coloredlogs on it.
    """
    logpath = os.path.abspath('get_tweets.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # create file handler which logs even debug messages
    fh = logging.FileHandler(logpath)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.INFO)
    
    l = logging.getLogger()
    l.setLevel(logging.DEBUG)
    l.addHandler(fh)
    l.addHandler(ch)

    ol = logging.getLogger('oathlib')
    ol.setLevel('INFO')
    ol.addHandler(fh)
    ol.addHandler(ch)
    req = logging.getLogger('requests')
    req.setLevel('INFO')
    req.addHandler(fh)
    req.addHandler(ch)
    ul = logging.getLogger('urllib3')
    ul.setLevel('WARNING')
    ul.addHandler(fh)
    ul.addHandler(ch)
    olib_oauth_rfc = logging.getLogger('oauthlib.oauth1.rfc5849')
    olib_oauth_rfc.setLevel('INFO')
    olib_oauth_rfc.addHandler(fh)
    olib_oauth_rfc.addHandler(ch)
    oath_lib_2 = logging.getLogger('requests_oauthlib.oauth1_auth')
    oath_lib_2.setLevel('INFO')
    oath_lib_2.addHandler(fh)
    oath_lib_2.addHandler(ch)

    return l
    
def load_credentials():
    """Load creds from a json file in the same folder. Using multiple allows you to rotate through just incase something times out.
    
    Returns:
        list: a list of dicts that contain the twitter creds
    """

    with open(CREDENTIAL_FILE) as fh:
        creds = json.load(fh)
    return creds

def get_tokens():
    """For each of the creds, get the access token and remaining requests
    
    Returns:
        list: List of dicts containing token data.
    """

    ret = []
    for c in load_credentials():
        t = get_token(c)
        rl = get_rate_limit_status(t)
        ret.append({
            "token": t,
            "remaining": rl['remaining'],
            "reset": rl['reset']
        })
    return ret

def get_token(config):
    """Makes the request to the twitter API for the oauth token.
    
    Args:
        config (dict): dict of consumer key, consumer secret, access token, access secret
    
    Returns:
        dict
    """

    b64_encoded = base64.b64encode("{}:{}".format(config['consumer_key'], config['consumer_secret']))
    post_headers = {
        'Authorization': 'Basic {0}'.format(b64_encoded),
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
    }
    res = requests.post(url='https://api.twitter.com/oauth2/token',
                            data={'grant_type': 'client_credentials'},
                            headers=post_headers)
    return res.json()['access_token']

def update_rate_limits():
    """Loop over the tokens, and check their current status.
    """

    global tokens
    for idx, t in enumerate(tokens):
        rl = get_rate_limit_status(t['token'])
        tokens[idx] = {
            "token": t['token'],
            "remaining": rl['remaining'],
            "reset": rl['reset']
        }
        

def get_rate_limit_status(token):
    """For a given token, call the rate limit API to get the remaining search requests.
    
    Args:
        token (str): THe token to check
    
    Returns:
        dict: the remaining requests and the reset timestamp
    """

    rate_limit = requests.get('https://api.twitter.com/1.1/application/rate_limit_status.json?resources=search', headers={'Authorization': 'Bearer {}'.format(token)})

    # Sometimes, if you are entirely out, it'll return a 429.
    if rate_limit.status_code == 429:
        return {'remaining': 0, 'reset': int(rate_limit.headers['x-rate-limit-reset'])}
    try:
        return rate_limit.json()['resources']['search']['/search/tweets']
    except KeyError, e:
        print rate_limit.json()
        raise

def wait_for_rate_limit(rl):
    """Sleep long enough that we have a token that is completely reset
    
    Args:
        rl (dict): The rate limit dict that we are waiting for.
    """

    to_sleep = rl['reset'] - int(time.time()) + 15
    while to_sleep > 0:
        logging.info('Waiting for rate-limit reset. {} seconds left'.format(to_sleep))
        time.sleep(60)
        to_sleep -= 60

def get_available_token():
    """Loop over the tokens and get the best choice.  If we dont have a good one, wait for one to reset.

    Returns:
        str: the best available token.
    """

    update_rate_limits()
    token_status = "Token Statuses:\n"
    for x in [(idx, x['remaining'], x['reset']) for idx, x in enumerate(tokens)]:\
        token_status += "Token {} - remaining: {} reset at {}\n".format(x[0], x[1], x[2])
    logger.info(token_status)
    available = sorted(tokens, key=lambda x: x['remaining'], reverse=True)[0]
    if available['remaining'] < 5:
        first_to_reset = sorted(tokens, key=lambda x: x['reset'])[0]
        wait_for_rate_limit(first_to_reset)
        return first_to_reset
    return available['token']

def write_json_out_to_file(dataset):
    """Write the dataset we just got out to a json file.  Written as a json object per line for use with Spark later.
    
    Args:
        dataset (list): a list of tweet objects.
    """

    with open(OUTPUT_LOCATION, 'a') as fh: 
        for d in dataset:
            fh.write(json.dumps(d) + '\n')

def get_them(token, query):
    """Gets the tweets for a given query.
    
    Args:
        token (str): The auth token
        query (str): the query to search for
    
    Returns:
        any
    """

    url = "https://api.twitter.com/1.1/search/tweets.json"

    r = requests.get(url,  headers={'Authorization': 'Bearer {}'.format(token)}, params=query)
    if not r.ok:
        print url
        print r.text
        print r
    r.raise_for_status()
    return r.json()

def get_next(token, next_params):
    """Instead of getting a specific query, we are getting the next set of results for a previously run query
    
    Args:
        token (str): the auth token
        next_params (str): the next param from the previous query
    
    Returns:
        any
    """

    url = "https://api.twitter.com/1.1/search/tweets.json"

    r = requests.get(url + next_params,  headers={'Authorization': 'Bearer {}'.format(token)})
    if not r.ok:
        print url
        print r.text
        print r
    r.raise_for_status()
    return r.json()

def get_start_id_from_file():
    """Get the starting ID from the text file.  See write_start_id
    
    Returns:
        int
    """

    with open("start_id") as fh:
        return int(fh.read())

def write_start_id(start_id):
    """Writes the Tweet Id out to a file.  We keep track of these after each pull so that, 
    if the script breaks or whatever, we dont have to start from scratch
    
    Args:
        start_id (int): the id to write out
    """

    old_start_id = get_start_id_from_file()
    if start_id > old_start_id:
        with open(START_ID_FILE, 'w') as fh:
            fh.write(str(start_id))


def download_tweets(start_id):
    """Get the first batch of tweets to start off this run. This is the entry point of the script
    
    Args:
        start_id (int): the start id
    """

    token = get_available_token()
    
    remaining = get_rate_limit_status(token)['remaining']

    query = {'q': BASE_QUERY, 'count': 100, 'result_type': 'recent'}
    them = get_them(token, query)

    last_status_id = max([x['id'] for x in them['statuses']])
    
    new_query = {'q': BASE_QUERY, "since_id": start_id, 'max_id': last_status_id, 'count': 100, 'result_type': 'mixed'}
    
    logger.info("Getting tweets from  {} to {}".format(start_id, last_status_id))
    
    tweets_so_far = 0

    r = requests.get("https://api.twitter.com/1.1/search/tweets.json", headers={'Authorization': 'Bearer {}'.format(token)}, params=new_query)
    write_json_out_to_file(r.json()['statuses'])
    tweets_so_far += len(r.json()['statuses'])
    
    while r.json()['search_metadata'].get('next_results'):
        r = requests.get("https://api.twitter.com/1.1/search/tweets.json" + r.json()['search_metadata'].get('next_results'), headers={'Authorization': 'Bearer {}'.format(token)})
        write_json_out_to_file(r.json()['statuses'])
        tweets_so_far += len(r.json()['statuses'])
        last_status_id = max([x['id'] for x in r.json()['statuses']])
        write_start_id(last_status_id)

        logger.info("Got {} tweets, wrote id {} to file...".format(tweets_so_far, last_status_id))
        remaining = remaining - 1
        if remaining < 10:
            token = get_available_token()
            remaining = get_rate_limit_status(token)['remaining']


logger = get_logger()
logger.info("Getting token...")
tokens = get_tokens()

logger.info("Got the token")

logger.info('Checking rate limit before we start...')


logger.info("Getting the first id from our file")
start_id = get_start_id_from_file()

logger.info("Getting the first result set...")
download_tweets(start_id)
logger.info("Done.")

