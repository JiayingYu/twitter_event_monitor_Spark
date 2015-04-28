#import dataset into RDD
sample_data = sc.textFile("/Users/jiayingyu/Dropbox/workSpace/twitterEventMonitor/sample_corpus.txt")
tweets_labeled = sample_data.map(lambda line: line.split("|"))
print tweets_labeled.first()

#split the tweets text into bag of words using NLTK
from nltk.tokenize import wordpunct_tokenize
import re
import nltk

def extract_words():
	


