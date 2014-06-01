import com.mongodb.casbah.Imports._
import collection.mutable.HashMap

class Vectorizer {

	private var words = new HashMap[String, Int]()
	
	def initAllWordsMap(mongoConn: MongoConnection, dbName: String, collectionName: String){
		val collectionTweets = mongoConn(dbName)(collectionName)
		
		collectionTweets.find().foreach{
			t => t.as[MongoDBList]("content").toList.foreach{
				case r: String =>
					if(!words.contains(r)){
						words += r -> words.size
					}
			}
		}
		println("[SUCCESS] Words amount: " + words.size)
	}

	//Creates vector. Last element of created vector is the label value
	//(tag,classification)
	
	def createVectorForContent(content : Array[String], label : Int) = {
		val vector = new Array[Int](words.size + 1)
		content.foreach { t =>
			if (words.contains(t)) {
				vector(words(t)) = 1	
			}
		}
		vector(vector.length-1) = label;
		vector
	}

}