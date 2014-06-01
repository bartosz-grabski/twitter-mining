import com.mongodb.casbah.Imports._
import JSON._

import scala.io.Source._
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object Preprocessor extends App {
	
	//config
	val stopwordsFile = "stopwords.csv"
	val tweetsFile = "tweets/twitter.english_tweet.template.json"
	
	//to prevent java.nio.charset.UnmappableCharacterException
	implicit val codec = Codec("UTF-8")
	codec.onMalformedInput(CodingErrorAction.REPLACE)
	codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  
	// get DB server connection
	val mongoConn = MongoConnection("localhost", 27017)

	// create a new DB and Collection if not present or use existing one
	val collectionTweets = mongoConn("twitter")("tweets")

	// drop create
	if(args.length > 0 && args(0) == "drop"){
		collectionTweets.dropCollection()
		println("[INFO] Dropping before insert")
	}
	
	val stripper = new StopWordsStripper()
	
	//load json file with tweets	
	val lines = scala.io.Source.fromURL(getClass.getResource(tweetsFile)).getLines()
	
	println("[INFO] Inserting...")
	for(line <- lines){
		val result =parseJSON(line)
		println("\t+ " + result.tweetid.toString.toLong)
		val newTweet = MongoDBObject(("tweet_id",result.tweetid.toString.toLong), ("content",stripper.strip(result.text.toString, stopwordsFile)))
		collectionTweets += newTweet
	}
	
	println("[SUCCESS] Tweets in DB: " + collectionTweets.size)
}