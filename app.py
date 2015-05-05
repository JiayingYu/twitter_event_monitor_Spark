from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import SVMWithSGD
from nltk.tokenize import wordpunct_tokenize
import re


conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)


#parse the a line of raw data into label, bag of words pair
def parse_line(line):
	fields = line.split("|||")
	fields[1].strip('- ')
	fields[1] = extract_words_wordpunct(fields[1])
	return fields

#test wordpunct_tokenize
#splited = wordpunct_tokenize(tweets_text.first())
"""
take one record of tweet text, transform it into bag of words
tokenize the tweets
delete nonsense words and words with length < 2
"""
"""
def extract_words(tweet):
	#import re
	tweet = tweet.replace('\r', ' ').lower()
	tweet = tweet.split(" ")
	for w in tweet:
		w.strip(',.#@ ')
	tweet = [w for w in tweet if re.search('[a-zA-Z]+', w) and len(w) > 1]
	return tweet
"""
#extract bag of words with nltk wordpunct_tokenizer
def extract_words_wordpunct(tweet):
	#import re
	#from nltk.tokenize import wordpunct_tokenize
	tweet = tweet.replace('\r', ' ').lower()
	tweet = wordpunct_tokenize(tweet)
	for w in tweet:
		w.strip(',.#@ ')
	tweet = [w for w in tweet if re.search('[a-zA-Z]+', w) and len(w) > 2]
	#bag_of_words = [w for w in tweet if w not in nonsense_words]
	return tweet

def construct_vector(words, features):
	vector = []
	for w in features:
		if (w in words):
			vector.append(1)
		else:
			vector.append(0)
	return vector

#construct labeled_point for each line of input
def construct_labeled_point(line, features):
	words = line[1]
	vector = []
	for w in features:
		if (w in words):
			vector.append(1)
		else:
			vector.append(0)
	return LabeledPoint(line[0], vector)

#extract features set from parsed data 
def feature_extraction(parsed_lines):
	nonsense_words = ["all", "are", "can", "and", "the", "https", "http", "co", "in", "this", "what", "so", "we", "me", "just", "was", "you", "that","an", "of", "it", "to", "is", "my","all", "very", "from", "99u", "our", "got", "don"]
	all_words = parsed_lines.map(lambda line: line[1]).flatMap(lambda x: x)
	all_words = all_words.filter(lambda x: x not in nonsense_words)
	word_count = all_words.map(lambda x: (x, 1.0)).reduceByKey(lambda x,y: x+y)
	#word_count_sorted = word_count.sortBy(lambda x: x[1], ascending = False)
	features = word_count.filter(lambda x: x[1] >= 4).map(lambda x: x[0]).collect()
	return features

#determin if the model prediction is correct for the single labeled_point
def is_correct(model, point):
	if(model.predict(point.features) == point.label): 
		return 1
	else: 
		return 0

def training(path):
	#import dataset into RDD
	raw_data = sc.textFile(path)
	#parse raw data into label bag-of-words pairs
	parsed_data = raw_data.map(lambda line: parse_line(line))
	#separate into training set and test set
	training_set, test_set = parsed_data.randomSplit([0.6, 0.4], 17)
	#get features for model training
	features = feature_extraction(training_set)
	labeled_points_training = training_set.map(lambda line: construct_labeled_point(line, features))
	labeled_points_test = test_set.map(lambda line: construct_labeled_point(line, features))
	#train logistic regression model
	lrModel = LogisticRegressionWithLBFGS.train(labeled_points_training)
	#train naive bayes model
	nbModel = NaiveBayes.train(labeled_points_training)
	svmModel = SVMWithSGD.train(labeled_points_training)
	return lrModel, nbModel, svmModel, labeled_points_test, features

def accuracy(model, labeled_points):
	num_correct_predict = labeled_points.map(lambda point: is_correct(model, point)).sum()
	accuracy = 1.0 * (num_correct_predict) / labeled_points.count()
	return accuracy

def precision(model, labeled_points):
	num_true_positives = labeled_points.filter(lambda point: point.label == 1 and model.predict(point.features)).count()
	num_false_positives = labeled_points.filter(lambda point: point.label == 0 and model.predict(point.features)).count()
	precision = 1.0 * num_true_positives / (num_true_positives + num_false_positives)
	return precision

def recall(model, labeled_points):
	num_true_positives = labeled_points.filter(lambda point: point.label == 1 and model.predict(point.features)).count()
	num_false_negative = labeled_points.filter(lambda point: point.label == 1 and not model.predict(point.features)).count()
	recall = 1.0 * num_true_positives / (num_true_positives + num_false_negative)
	return recall

def print_stats(train_result, dataset_name):
	acc_lr = accuracy(train_result[0], train_result[3])
	acc_svm = accuracy(party_result[2], party_result[3])
	acc_nb = accuracy(train_result[1], train_result[3])

	prec_lr = precision(train_result[0], train_result[3])
	recall_lr = recall(train_result[0], train_result[3])

	prec_nb = precision(train_result[1], train_result[3])
	recall_nb = recall(train_result[1], train_result[3])

	prec_svm = precision(train_result[2], train_result[3])
	recall_svm = recall(train_result[2], train_result[3])

	fd = open(stats_out, "a")
	fd.write("====================================\n")
	fd.write(dataset_name + "\n\n")

	fd.write("LR accuracy: " + str(acc_lr) + "\n")
	fd.write("LR Precision: " + str(prec_lr) + "\n")
	fd.write("LR Recall: " + str(recall_lr) + "\n\n")

	fd.write("NB accuracy: " + str(acc_nb) + "\n")
	fd.write("NB Precision: " + str(prec_nb) + "\n")
	fd.write("NB Recall: " + str(recall_nb) + "\n\n")

	fd.write("SVM accuracy: " + str(acc_svm) + "\n")
	fd.write("SVM Precision:" + str(prec_svm) + "\n")
	fd.write("SVM Recall: " + str(recall_svm) + "\n")

	fd.write("\nchange the threshold to 0.8 for LR and SVM\n\n")

	train_result[0].setThreshold(0.8)
	train_result[2].setThreshold(0.8)

	prec_lr_2 = precision(train_result[0], train_result[3])
	recall_lr_2 = recall(train_result[0], train_result[3])

	prec_svm_2 = precision(train_result[2], train_result[3])
	recall_svm_2 = recall(train_result[2], train_result[3])

	fd.write("LR Precision: " + str(prec_lr_2)  + "\n")
	fd.write("LR Recall: " + str(recall_lr_2)  + "\n\n")

	fd.write("SVM Precision: " + str(prec_svm_2)  + "\n")
	fd.write("SVM Recall: " + str(recall_svm_2)  + "\n")	

	fd.write("====================================\n\n")

	fd.close()

stats_out = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/stats.txt"
f = open(stats_out, "w")
f.write("Results\n")
f.close()

party_file = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/party_data_labeled.txt"
party_result = training(party_file)

# acc_lr_party = accuracy(party_result[0], party_result[3])
# acc_nb_party = accuracy(party_result[1], party_result[3])
# acc_svm_party = accuracy(party_result[2], party_result[3])
# party_result[0].setThreshold(0.8)
# party_result[2].setThreshold(0.8)
# prec_lr_party = precision(party_result[0], party_result[3])
# recall_lr_party = recall(party_result[0], party_result[3])
# prec_nb_party = precision(party_result[1], party_result[3])
# recall_nb_party = recall(party_result[1], party_result[3])
# prec_svm_party = precision(party_result[2], party_result[3])
# recall_svm_party = recall(party_result[2], party_result[3])


traffic_file = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/traffic_data_labeled.txt"
traffic_result = training(traffic_file)
# acc_lr_traf = accuracy(traffic_result[0], traffic_result[3])
# acc_nb_traf = accuracy(traffic_result[1], traffic_result[3])

#print acc_lr_traf
#print acc_nb_traf

sale_file = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/sale_data_labeled.txt"
sale_result = training(sale_file)


# acc_lr_sale = accuracy(sale_result[0], sale_result[3])
# acc_nb_sale = accuracy(sale_result[1], sale_result[3])

# print ("Party dataset:")
# print ("LR accuracy: " + str(acc_lr_party))
# print ("NB accuracy: " + str(acc_nb_party))
# print ("SVM accuracy: " + str(acc_svm_party))
# print ("Precision of LR: " + str(prec_lr_party))
# print ("Recall of LR: " + str(recall_lr_party))
# print ("Precision of NB: " + str(prec_nb_party))
# print ("Recall of NB: " + str(recall_nb_party))
# print ("Precision of SVM: " + str(prec_svm_party))
# print ("Recall of SVM: " + str(recall_svm_party))

'''print the result'''
print_stats(party_result, "Party dataset")
print_stats(traffic_result, "Traffic dataset")
print_stats(sale_result, "Sale dataset")


''''''''''''''''compute distance of two points'''''''''''
import math

#distance between two points in miles
def distance(lat1, long1, lat2, long2):
    degrees_to_radians = math.pi/180.0
        
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
        
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
        
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    return arc * 3963.1676

''''''''''''''''stream api'''''''''''''''''''''''''''''
import tweepy
import json

class StreamListener(tweepy.StreamListener):
	def __init__(self):
		super(StreamListener, self).__init__()		# Not sure if needed
    	# self.tweetCount = 0							# Tweets tracked
    	# self.tweetThresh = 100						# Tweet tracking limit 


	def on_status(self,tweet):
		text = tweet.text
		# if tweet.coordinates:
		# 	d = math.ceil(distance(tweet.coordinates["coordinates"][1], tweet.coordinates["coordinates"][0], geo[0], geo[1]))
		# 	if d < 10000000:
		# 		bag_of_words = extract_words_wordpunct(text)
		# 		vector = construct_vector(bag_of_words, sale_result[3])
		# 		predict = sale_result[0].predict(vector)
		# 		if (predict):
		# 			print text.encode('utf-8')

		bag_of_words = extract_words_wordpunct(text)
		vector = construct_vector(bag_of_words, traffic_result[4])
		predict = traffic_result[2].predict(vector)
		if (predict):
			print text.encode('utf-8') + "\n-----------------------------\n"

	def on_error(self, status_code):
		print "Error: " + repr(status_code)
		return False

consumer_key = "kfWvjLrk9pV9fJBwBmPq6zQz8";
consumer_secret = "eBCpt1JZHtzY3EVzOk8GRcmVA99oVo6la8mKRMwrYdLhoaYGQ2";
access_token = "3003809134-O726b4UnkM4tDxMECl2KuLpsHBIbD981cgycsT3";
access_token_secret = "YsPxVNzqhWBy4leGdZLmBYWI6VifnB2R6LfECz3CuD6kI";

auth1 = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth1.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth1)
#washington square park 
geo = [40.7300, -73.9950] #lan #long
lan = ['en']
key_words_sale = ["sale", "promotion", "deal", "discount"]
key_words_traffic = ["traffic incident","jam","collision","interstate","detour"]
key_words_party = ['party', 'beer', 'fire']

l = StreamListener()
streamer = tweepy.Stream(auth = auth1,listener = l)
#streamer.filter(track = key_words_party,languages = lan)
streamer.filter(track = key_words_traffic,languages = lan)
#streamer.filter(track = key_words_sale,languages = lan)

