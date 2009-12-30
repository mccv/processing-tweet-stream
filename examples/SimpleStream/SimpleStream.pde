import com.twitter.processing.*;

// this stores how many tweets we've gotten
int tweets = 0;
// and this stores the text of the last tweet
String tweetText = "";

void setup() {
  size(800,100);
  // set up fonts
  PFont font = loadFont("CharcoalCY-24.vlw");
  textFont(font, 24);
  // set up twitter stream object
  TweetStream s = new TweetStream(this, "stream.twitter.com", 80, "1/statuses/sample.json", "your-twitter-user", "your-twitter-password");
  s.go();
}

void draw() {
  background(0);
  fill(255, 0, 0);
  // draw a box as many pixels wide as the number of tweets we've seen
  rect(20, 20, tweets, 20);
  // and draw the text of the last tweet
  text(tweetText, 10, 70);
}

// called by twitter stream whenever a new tweet comes in
void tweet(Status tweet) {
  // print a message to the console just for giggles if you like
  // println("got tweet " + tweet.id());

  // store the latest tweet text
  tweetText = tweet.text();
  // bump our tweet count by one
  tweets += 1;
}
