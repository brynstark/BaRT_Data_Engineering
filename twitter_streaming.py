# This is adapted from Brian's code here:
# https://github.com/brianspiering/fun_with_twitter/blob/master/tweepy_writer/demo_tweepy_writer.ipynb

import json
import os
import sys

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

my_consumer_key = '[]'
my_consumer_secret = '[]'
my_access_token = '[]'
my_access_token_secret = '[]'

auth = OAuthHandler(my_consumer_key, my_consumer_secret)
auth.set_access_token(my_access_token, my_access_token_secret)

class WriteToDiskListener(StreamListener):
    """Write stream listener to disk with limited number of Tweets.
    """

    def __init__(self, filename, limit=5):
        self.counter = 0
        self.filename = filename
        self.limit = limit
        
    def on_data(self, data):
        "If under limit, write received data to disk."
        while self.counter < self.limit:
            try:
                with open(self.filename.lower()+'.json', 'a') as f:
                    f.write(data)
                self.counter += 1
                if (self.counter % 250) == 0:
                    print('250 Tweets')
                return True
            except BaseException as e:
                print("Error on_data: {}".format(e))
            return True
        else:
            return False
 
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    # Track is a list of search terms to stream.
    track = ['12th st oakland', '19th st oakland', '16th st mission', '24th st mission', 'ashby', 'berkeley', 'north berkeley',
             'richmond', 'el cerrito', 'balboa park', 'bayfair', 'san leandro', 'castro valley', 'civic center', 'powell st',
             'daly city', 'concord', 'coliseum', 'colma', 'dublin pleasanton', 'embarcadero', 'fremont', 'fruitvale', 
             'glen park sf', 'hayward', 'lafayette', 'lake merritt', 'macarthur', 'millbrae', 'montgomery', 'oakland airport',
             'sfo', 'orinda', 'pittsburgh bay point', 'pleasant hill', 'rockridge', 'san bruno', 'san francisco airport',
             'union city', 'walnut creek']
    # track = ['san francisco', 'oakland', 'bay area', 'richmond', 'berkeley', 'daly city', 'sf', 'frisco', 'sanfran', 'san fran']
    filename = "_".join([item.lower() for item in track[:1]])

    # # Remove existing file of tweets
    # try:
    #     os.remove(filename+'.json')
    # except OSError:
    #     pass

    listener = WriteToDiskListener(filename=filename, 
                                    limit=2000)
    stream = Stream(auth, listener)

    try:
        stream.filter(track=track,
                      languages=['en'])
    except:
        stream.disconnect()