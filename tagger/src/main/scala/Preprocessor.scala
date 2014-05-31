import com.mongodb.casbah.Imports._
import scala.util.parsing.json._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object DBfiller extends App {

  //to prevent java.nio.charset.UnmappableCharacterException
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  // get DB server connection
  val mongoConn = MongoConnection("localhost", 27017)

  // create a new DB and Collection if not present or use existing one
  val collectionTweets = mongoConn("twitter")("tweets")

  // delete the collection
  collectionTweets.dropCollection


  //load json file with tweets
  val lines = scala.io.Source.fromFile("src/main/resources/tweets/twitter.english_tweet.json").getLines()

  for(line <- lines){
	println(line);
    val result =JSON.parseFull(line)

    result match {
		
      case Some(e) => {
		val newTweet = MongoDBObject(("tweet_id",result("$oid").asInstanceOf[String]), ("content",result("text")))
		collectionTweets += newTweet
		println(e) // => Map(name -> Naoki, lang -> List(Java, Scala))
      }
	  case None => println("Failed.")
	}
  }
  
  
}