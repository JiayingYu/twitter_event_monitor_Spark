#import dataset into RDD
sample_data = sc.textFile("/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/sample_corpus.txt")
#split data into two fields: label and tweet text
tweets_labeled = sample_data.map(lambda line: line.split("|"))
print tweets_labeled.first()
#extract the tweet text field
tweets_text = tweets_labeled.map(lambda line: line[1])

#split the tweets text into bag of words using NLTK
from nltk.tokenize import wordpunct_tokenize
import nltk

"""
take one record of tweet text, transform it into bag of words
tokenize the tweets
delete nonsense words and words with length < 3
"""


nonsense_words = ["and", "the", "http", "co"]
splited = wordpunct_tokenize(tweets_text.first())

def extract_words(tweet):
	import re
	tweet = tweet.replace('\r', ' ').lower()
	tweet = tweet.split(" ")
	for w in tweet:
		w.strip(',.#@ ')
	tweet = [w for w in tweet if re.search('[a-zA-Z]+', w) and len(w) > 1]
	return tweet

#with nltk tokenizer
def extract_words2(tweet):
	import re
	from nltk.tokenize import wordpunct_tokenize
	tweet = tweet.replace('\r', ' ').lower()
	tweet = wordpunct_tokenize(tweet)
	for w in tweet:
		w.strip(',.#@ ')
	tweet = [w for w in tweet if re.research('[a-zA-Z]+', w), and len(w) > 1]
	return tweet

label_words_pairs1 = tweets_labeled.map(lambda fields: [fields[0], extract_words2(fields[1])])
all_words = label_words_pairs1.map(lambda fields: fields[1]).flatMap(lambda x : x)
#all_words = sc.parallelize()
word_count = all_words.map(lambda x: (x, 1.0)).reduceByKey(lambda a,b: a + b)
#sort the words by count descending
word_count_sorted = word_count.sortBy(lambda x: x[1], ascending = False)
#extract features: words apear more than 4 times
features = word_count.filter(lambda x: x[1] >= 4).map(lambda x: x[0]).collect()

#construct vector for each line.
#parameter words: bag of words of each tweet text
#return: vector of features
def construct_vector(words):
	vector = []
	for w in features:
		if (w in words):
			vector.append(1)
		else:
			vector.append(0)
	return vector

def construct_labeled_point(line):
	words = line[1]
	vector = []
	for w in features:
		if (w in words):
			vector.append(1)
		else:
			vector.append(0)
	return LabeledPoint(line[0], vector)

from pyspark.mllib.regression import LabeledPoint
#create the training data
training_data = label_words_pairs1.map(lambda fields: [fields[0], construct_vector(fields[1])])
training_data = label_words_pairs1.map(lambda fields: construct_labeled_point(fields))

#train logistic regression model
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
lrModel = LogisticRegressionWithLBFGS.train(training_data)

