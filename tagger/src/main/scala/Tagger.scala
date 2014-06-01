import com.mongodb.casbah.Imports._


object Tagger extends App {

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