import com.mongodb.casbah.Imports._

object DBfiller extends App {
  // get DB server connection
  val mongoConn = MongoConnection("localhost", 27017)

  // create a new DB and Collection if not present or use existing one
  val collectionTweets = mongoConn("twitter")("tweets")

  // delete the collection
  collectionTweets.dropCollection

  // add 1st Document
  val newTweet = MongoDBObject(("tweet_id",1), ("content","content"), ("in_reply_to",2), ("hashtags","asd asd asd"), ("user_mentions", "asd"))
  collectionTweets += newTweet

  // create an index
  collectionTweets.ensureIndex("tweet_id") // there is also createIndex

  // query interface
  // number of docs
  println("Number of docs in collection: " + collectionTweets.size)

  // distinct users
  println("distinct by hashtags:")
  collectionTweets.distinct("hashtags") foreach (println _)

  // all docs
  println("all docs:")
  collectionTweets.find foreach (println _)

  // find exact document
  println("Searching for: " + newTweet)
  collectionTweets.find(newTweet) foreach (println _)

  // find by column/attribute
  val q = MongoDBObject(("tweet_id",1))
  println("Find one doc by attribute: " + collectionTweets.findOne(q))

}