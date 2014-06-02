import com.mongodb.casbah.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import JSON._
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import com.mongodb.hadoop.MongoInputFormat


object Tagger extends App {

    val sc = new SparkContext("local","Twitter tagger")
    
    // Spark Mongo/Hadoop config

    val mongoHadoopConfig = new Configuration()
    mongoHadoopConfig.set("mongo.input.uri", "mongodb://127.0.0.1:27017/twitter.tweets")
    mongoHadoopConfig.set("mongo.output.uri", "mongodb://127.0.0.1:27017/twitter.tweets_output")



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
	// should do it for unlabeled test data (label == 0)

	println("[SUCCESS] created vectors for tweets")
	

	val mongoRDD = sc.newAPIHadoopRDD(mongoHadoopConfig,classOf[MongoInputFormat],classOf[Object],classOf[BSONObject])

	val trainingData = mongoRDD.map { bson =>
		val vector = bson._2.get("content").asInstanceOf[BasicDBList].toArray
		LabeledPoint(vector(vector.length-1).asInstanceOf[Int].toDouble,Vectors.dense(vector.slice(0,vector.length-1).map(_.asInstanceOf[Int].toDouble)))
	}

	trainingData.foreach { t=>
		println(t.toString)
	}

	val numIterations = 40
	val model = SVMWithSGD.train(trainingData, numIterations)

}