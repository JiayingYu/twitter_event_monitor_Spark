from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import NaiveBayes
from nltk.tokenize import wordpunct_tokenize
import re

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

#this function is not used
def construct_vector(words):
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
	return lrModel, nbModel, labeled_points_test

def accuracy(model, labeled_points):
	num_correct_predict = labeled_points.map(lambda point: is_correct(model, point)).sum()
	accuracy = double(num_correct_predict) / labeled_points.count()
	return accuracy

party_file = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/party_dataset.txt"
lr_model_party, nb_model_party, lp_test_party = training(party_file)
acc_lr_party = accuracy(lr_model_party, lp_test_party)
acc_nb_party = accuracy(nb_model_party, lp_test_party)
print ("Accuracy of LogisticRegression Model for Party: " + str(lr_accuracy))
print ("Accuracy of NaiveBayes Model for Party: " + str(nb_accuracy))

traffic_file = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/traffic_data.txt"
lr_traf, nb_traf, lp_test_traf = training(traffic_file)
acc_lr_traf = accuracy(lr_traf, lp_test_traf)
acc_nb_traf = accuracy(nb_traf, lp_test_traf)
print acc_lr_traf
print acc_nb_traf

sale_file = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/sale_data_raw.txt"
lr_sale, nb_sale, lp_test_sale = training(sale_file)
acc_lr_sale = accuracy(lr_sale, lp_test_sale)
acc_nb_sale = accuracy(nb_sale, lp_test_sale)
print acc_lr_sale
print acc_nb_sale

"""
file_path = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/party_dataset.txt"
#import dataset into RDD
raw_data = sc.textFile(file_path)
#parse raw data into label bag-of-words pairs
parsed_data = raw_data.map(lambda line: parse_line(line)).collect()
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
"""

"""
lrModel.predict(training_data.first().features)
predictions = lrModel.predict(training_data.map(lambda lp: lp.features))
predictions.take(20)
"""
