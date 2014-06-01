import com.mongodb.casbah.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import JSON._
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

object Tagger extends App {

	//val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    //val sc = new SparkContext(conf)
    val dbName = "twitter"
    val collectionName = "tweets"
    val testCollectionLabeled = "test_tweets_labeled"
    val testCollectionUnlabeled = "test_tweets_unlabeled"
	// get DB server connection
	val mongoConn = MongoConnection("localhost", 27017)

	val vectorizer = new Vectorizer()
	
	// init map with all words in collection
	vectorizer.initAllWordsMap(mongoConn, dbName, collectionName)
	
	// create vector
	vectorizer.createVectorForContent(Array("ronnie","how"),1).foreach {
		println _
	}

	val tweets = mongoConn(dbName)(collectionName)
	println("[INFO] creating vectors from tweets contents")

	tweets.foreach { t =>
		var content = parseJSON(t("content").toString).map(_.toString).toArray
		val vector = vectorizer.createVectorForContent(content,1)
		t("content") = vector
		tweets.save(t)
	}

	tweets.foreach { t =>

	}

	// should do it for unlabeled test data (label == 0)

	println("[SUCCESS] created vectors for tweets")
	

	val trainingData = tweets.map { t => 
		var content = parseJSON(t("content").toString).map(_.toString).toArray
		content.foreach {
			println _
		}
		LabeledPoint(content.last.toDouble,content.slice(0,content.length-1).map(_.toDouble))
	}



	//should do it for unlabeled to trainingData += ...


}