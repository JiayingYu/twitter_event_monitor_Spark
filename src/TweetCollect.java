
// add twitter4j-core-4.0.3.jar to classpath
//This program collects 200 tweets based on the Query object

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
public class TweetCollect {
	public static void main(String[] args) throws TwitterException {
		final String CONSUMER_KEY = "kfWvjLrk9pV9fJBwBmPq6zQz8";
		final String CONSUMER_SECRET = "eBCpt1JZHtzY3EVzOk8GRcmVA99oVo6la8mKRMwrYdLhoaYGQ2";
		final String ACCESS_TOKEN = "3003809134-O726b4UnkM4tDxMECl2KuLpsHBIbD981cgycsT3";
		final String ACCESS_TOKEN_SECRET = "YsPxVNzqhWBy4leGdZLmBYWI6VifnB2R6LfECz3CuD6kI";
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(CONSUMER_KEY);
		cb.setOAuthConsumerSecret(CONSUMER_SECRET);
		cb.setOAuthAccessToken(ACCESS_TOKEN);
		cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
		
		Twitter twitter = new TwitterFactory(cb.build()).getInstance();
	  Query query = new Query("movie");
	  query.setCount(100);
	  System.out.println(query.getCount());
	  QueryResult result = twitter.search(query);
	  for (Status status : result.getTweets()) {
	      System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
	  }
	  
	  if (result.hasNext()) {
	  	query = result.nextQuery();
	  }
	  
	  result = twitter.search(query);
	  
	  for (Status status : result.getTweets()) {
	      System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
	  }
	  
	}
}
	
