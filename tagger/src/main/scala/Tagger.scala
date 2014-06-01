import com.mongodb.casbah.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Tagger extends App {

	val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
	// get DB server connection
	val mongoConn = MongoConnection("localhost", 27017)

	val vectorizer = new Vectorizer()
	
	// init map with all words in collection
	vectorizer.initAllWordsMap(mongoConn, "twitter", "tweets")
	
	// create vector
	vectorizer.createVectorForContent(Array("ronnie","how"),1).foreach {
		println _
	}
}