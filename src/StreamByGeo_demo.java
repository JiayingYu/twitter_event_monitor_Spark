    //source: http://stackoverflow.com/questions/27122615/twitter4j-search-api-tweets-based-on-location-and-time-interval

    StatusListener listener = new StatusListener(){
        public void onStatus(Status status) {
            //if (status.getText().contains)
            if(status.getUser().getLang().equalsIgnoreCase("en")
                    || status.getUser().getLang().equalsIgnoreCase("en_US")) {
                System.out.println(status.getUser().getName() + " :: " + status.getText() + " :: " + status.getGeoLocation());
            }
        }
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
        public void onException(Exception ex) {
            ex.printStackTrace();
        }
        public void onScrubGeo(long arg0, long arg1) {

        }
        public void onStallWarning(StallWarning arg0) {

        }
    };

    ConfigurationBuilder config = new ConfigurationBuilder();
    config.setOAuthConsumerKey("");
    config.setOAuthConsumerSecret("");
    config.setOAuthAccessToken("");
    config.setOAuthAccessTokenSecret("");

    TwitterStream twitterStream = new TwitterStreamFactory(config.build()).getInstance();
    twitterStream.addListener(listener);
    FilterQuery query = new FilterQuery();
    // New Delhi India
    double lat = 28.6;
    double lon = 77.2;

    double lon1 = lon - .5;
    double lon2 = lon + .5;
    double lat1 = lat - .5;
    double lat2 = lat + .5;

    double box[][] = {{lon1, lat1}, {lon2, lat2}};

    query.locations(box);

    String[] trackArray = {"iphone"}; 
    query.track(trackArray);
    twitterStream.filter(query);