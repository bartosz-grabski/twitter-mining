from twython import Twython, TwythonError
from data_model import Tweet

import random
import sys
import json

TWEETS_LIMIT = 200
username = sys.argv[1]

twitterKeys = [ x.strip().split(',') for x in open('auth').readlines() ]

twitter = Twython(*random.choice(twitterKeys))
try:
    timeline = twitter.get_user_timeline(screen_name=username,
                                         count=TWEETS_LIMIT)
except TwythonError as e:
    print e

print json.dumps(timeline,
                 indent=4,
                 separators=(',', ': '))

