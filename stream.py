import tweepy
import json

consumer_key = "kfWvjLrk9pV9fJBwBmPq6zQz8";
consumer_secret = "eBCpt1JZHtzY3EVzOk8GRcmVA99oVo6la8mKRMwrYdLhoaYGQ2";
access_token = "3003809134-O726b4UnkM4tDxMECl2KuLpsHBIbD981cgycsT3";
access_token_secret = "YsPxVNzqhWBy4leGdZLmBYWI6VifnB2R6LfECz3CuD6kI";

auth1 = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth1.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth1)
#washington square park
geo = [-75.1914,39.7088,-72.6406,41.5235]
lan = ['en']
key_words = ['sale', 'promotion', "deal", "discount"]

class StreamListener(tweepy.StreamListener):
	def __init__(self):
        super(StreamListener, self).__init__()		# Not sure if needed
        self.tweetCount = 0							# Tweets tracked
        self.tweetThresh = 100						# Tweet tracking limit 

	def on_status(self,tweet):
		text = tweet.text
		parsed_text = parse_line()
		cur_features = construct_vector(parsed_text)
		predict = lr_sale.predict(cur_features)
		if (predict):
			print(text)

	def on_error(self, status_code):
        print "Error: " + repr(status_code)
        return False
    #end on_error

	if __name__=='__main__':
    	l = StreamListener()
    	streamer = tweepy.Stream(auth=auth1,listener=l)
    	streamer.filter(track = key_words,languages=setLanguages)