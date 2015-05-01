from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import NaiveBayes
from nltk.tokenize import wordpunct_tokenize
import re


training_file_path = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/labeled_party_training_set.txt"
#import dataset into RDD
raw_data = sc.textFile(training_file_path)
#split data into two fields: label and tweet text
tweets_labeled = raw_data.map(lambda line: line.split("|||")).map(lambda fields: [fields[0], fields[1].strip('- ')])
print tweets_labeled.first()
#extract the tweet text field
tweets_text = tweets_labeled.map(lambda line: line[1])


#test wordpunct_tokenize
#splited = wordpunct_tokenize(tweets_text.first())
"""
take one record of tweet text, transform it into bag of words
tokenize the tweets
delete nonsense words and words with length < 2
"""
def extract_words(tweet):
	#import re
	tweet = tweet.replace('\r', ' ').lower()
	tweet = tweet.split(" ")
	for w in tweet:
		w.strip(',.#@ ')
	tweet = [w for w in tweet if re.search('[a-zA-Z]+', w) and len(w) > 1]
	return tweet

#extract bag of words with nltk wordpunct_tokenizer
def extract_words_wordpunct(tweet):
	#import re
	#from nltk.tokenize import wordpunct_tokenize
	tweet = tweet.replace('\r', ' ').lower()
	tweet = wordpunct_tokenize(tweet)
	for w in tweet:
		w.strip(',.#@ ')
	tweet = [w for w in tweet if re.search('[a-zA-Z]+', w) and len(w) > 2]
	bag_of_words = [w for w in tweet if w not in nonsense_words]
	return bag_of_words

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
def construct_labeled_point(line):
	words = line[1]
	vector = []
	for w in features:
		if (w in words):
			vector.append(1)
		else:
			vector.append(0)
	return LabeledPoint(line[0], vector)

#features extraction
nonsense_words = ["and", "the", "https", "http", "co", "in", "this", "what", "so", "we", "me", "off", "just", "for", "was", "with", "you", "that","an", "of", "on", "it", "to", "is", "my"]
label_words_pairs = tweets_labeled.map(lambda fields: [fields[0], extract_words_wordpunct(fields[1])])
label_words_pairs.first()
all_words = label_words_pairs.map(lambda fields: fields[1]).flatMap(lambda x : x)
word_count = all_words.map(lambda x: (x, 1.0)).reduceByKey(lambda a,b: a + b)
#sort the words by count descending
word_count_sorted = word_count.sortBy(lambda x: x[1], ascending = False)
#extract features: words apear more than 4 times
features = word_count.filter(lambda x: x[1] >= 4).map(lambda x: x[0]).collect()
print features

#create the training data
training_data = label_words_pairs.map(lambda fields: construct_labeled_point(fields))
training_data.take(20)

#train logistic regression model
lrModel = LogisticRegressionWithLBFGS.train(training_data)
#train naive bayes model
nbModel = NaiveBayes.train(training_data)
#predict the first
lrModel.predict(training_data.first().features)
predictions = lrModel.predict(training_data.map(lambda lp: lp.features))
predictions.take(20)

#determin if the model prediction is correct for the single labeled_point
def is_correct(model, point):
	if(model.predict(point.features) == point.label): 
		return 1
	else: 
		return 0

#process test data set
test_file_path = "/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/party_test_set"
raw_test_data = sc.textFile(test_file_path)
test_data_labeled = raw_test_data.map(lambda line: line.split("|||")).map(lambda fields: [fields[0], fields[1].strip('- ')])
test_data_labeled.take(5)
test_label_points = test_data_labeled.map(lambda fields: construct_labeled_point(fields))
test_label_points.take(5)
#caculate accuracy
num_lr_correct_predict = test_label_points.map(lambda point: is_correct(lrModel, point)).sum()
lr_accuracy = double(num_lr_correct_predict) / test_data_labeled.count()
print lr_accuracy

num_nb_correct_predict = test_label_points.map(lambda point: is_correct(nbModel, point)).sum()
nb_accuracy = double(num_nb_correct_predict) / test_data_labeled.count()
print lr_accuracy


