import com.mongodb.casbah.Imports._
import JSON._

import scala.io.Source._
import java.nio.charset.CodingErrorAction
import scala.io.Codec

class TrainingDataPreprocessor() {

	//to prevent java.nio.charset.UnmappableCharacterException
	implicit val codec = Codec("UTF-8")
	codec.onMalformedInput(CodingErrorAction.REPLACE)
	codec.onUnmappableCharacter(CodingErrorAction.REPLACE)


	def process(labeledFile:String, unlabeledFile:String, trainingCollectionName:String, dbName:String, stopwordsFile:String) = {
		

		// get DB server connection
		val mongoConn = MongoConnection("localhost", 27017)

		// create a new DB and Collection if not present or use existing one
		val collectionTweets = mongoConn(dbName)(trainingCollectionName)

		collectionTweets.dropCollection()

		val stripper = new StopWordsStripper(stopwordsFile)

		println("[INFO] Processing labeled file")

		processFile(labeledFile,1,stripper,collectionTweets)

		println("[INFO] Processing unlabeled file")

		processFile(unlabeledFile,0,stripper,collectionTweets)
		
	}


	private def processFile(filename:String, label:Int, stripper: StopWordsStripper, collection : MongoCollection) {
		
		val lines = scala.io.Source.fromURL(getClass.getResource(filename)).getLines()

		for(line <- lines) {
			val result =parseJSON(line)
			//println("\t+ " + result.tweetid.toString.toLong)
			val newTweet = MongoDBObject(("tweet_id",result.tweetid.toString.toLong), ("content",stripper.strip(result.text.toString.toLowerCase())), ("label",label))
			collection += newTweet
		}

		println("[INFO] Finished parsing file "+filename)
	}

}