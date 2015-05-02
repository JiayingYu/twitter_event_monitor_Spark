// add twitter4j-core-4.0.3.jar to classpath
//This program collects 200 tweets based on the Query object

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetCollect {
	private static final String CONSUMER_KEY = "kfWvjLrk9pV9fJBwBmPq6zQz8";
	private static final String CONSUMER_SECRET = "eBCpt1JZHtzY3EVzOk8GRcmVA99oVo6la8mKRMwrYdLhoaYGQ2";
	private static final String ACCESS_TOKEN = "3003809134-O726b4UnkM4tDxMECl2KuLpsHBIbD981cgycsT3";
	private static final String ACCESS_TOKEN_SECRET = "YsPxVNzqhWBy4leGdZLmBYWI6VifnB2R6LfECz3CuD6kI";
	private static BufferedWriter bw; // write results into a file
	private static Twitter twitter;

	public static void main(String[] args) throws TwitterException {
		// set up configuration
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(CONSUMER_KEY);
		cb.setOAuthConsumerSecret(CONSUMER_SECRET);
		cb.setOAuthAccessToken(ACCESS_TOKEN);
		cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
		twitter = new TwitterFactory(cb.build()).getInstance();

		//collectParty();
		collectEmergency();
		

	}

	private static void collectEmergency() throws TwitterException {
		Query queryEmergency = new Query("delay OR collision OR explosion -filter:retweets");
		GeoLocation nyc = new GeoLocation(40.758996, -73.978679);
		//queryEmergency.since("2015-02-01");
		queryEmergency.setGeoCode(nyc, 100, Query.MILES);
		String emergencyOutput = "emergency_dataset.txt";
		collect(queryEmergency, emergencyOutput);
		
	}

	public static void collectParty() throws TwitterException {
	//collect tweets about party near washington square park
			GeoLocation washingtonSqr = new GeoLocation(40.730468, -73.997701);
			// excludes the duplicate tweets by filter out the retweets
			Query queryParty = new Query("party OR beer OR fire -filter:retweets");
			queryParty.setGeoCode(washingtonSqr, 5.0, Query.MILES);
			String partyOutput = "party_dataset_raw.txt";
			collect(queryParty, partyOutput);
	}

	//search and collect dataset based on given query, location and write to the 
	//output file path
	public static void collect(Query query, String outputFile) throws TwitterException {
		// create buffered writer to write results into dataset file
		try {
			File out = new File(outputFile);
			if (out.exists()) {
				out.createNewFile();
			}

			FileWriter fw = new FileWriter(out.getAbsoluteFile());
			bw = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}

		query.setLang("en");
		query.setCount(100); // set number of tweets returned per page to 100

		writeToFile(query);
		System.out.println("Finish collecting");

		// close buffered writer
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// print the query result to console
	public static void printSearchResultsPerPage(QueryResult result) {
		for (Status status : result.getTweets()) {
			System.out.println("@" + status.getUser().getScreenName() + ":"
					+ status.getText());
		}
	}

	public static void writeToFile(Query query) throws TwitterException {
		QueryResult result = twitter.search(query);
		// collect 300 tweets
		for (int i = 0; i < 4; i++) {
			for (Status status : result.getTweets()) {
				String content = status.getText();
				content.replaceAll("\\r", " ");
				try {
					bw.write(content + "----\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (result.hasNext()) {
				query = result.nextQuery();
				result = twitter.search(query);
			} else {
				System.out.println("Not enough data collected");
				break;
			}
		}
	}
}
