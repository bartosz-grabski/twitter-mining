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
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}


object Tagger extends App {

	// config properties
	val dbName = "twitter"
    val collectionName = "tweets"
    val vectorCollection = "tweets_vectors"
    val outputCollection = "tweets_labeled"
    val labeledTrainingFile = "tweets/training/labeled.json"
    val unlabeledTrainingFile = "tweets/training/unlabeled.json"
    val trainingCollection = "tweets_training"
    val stopWordsFile = "stopwords.csv"

    val sc = new SparkContext("local","Twitter tagger")
    
    // Spark Mongo/Hadoop config

    val mongoTrainingConfig = new Configuration()
    mongoTrainingConfig.set("mongo.input.uri", "mongodb://127.0.0.1:27017/twitter."+trainingCollection)
    mongoTrainingConfig.set("mongo.output.uri", "mongodb://127.0.0.1:27017/twitter."+outputCollection) //output is bogus ?

    val mongoTweetsConfig = new Configuration()
    mongoTweetsConfig.set("mongo.input.uri", "mongodb://127.0.0.1:27017/twitter."+vectorCollection)
    mongoTweetsConfig.set("mongo.output.uri", "mongodb://127.0.0.1:27017/twitter."+outputCollection) //output is bogus ?


	// get DB server connection
	val mongoConn = MongoConnection("localhost", 27017)
	
	// create vectorizer object
	val vectorizer = new Vectorizer()
	val trainingDataPreprocessor = new TrainingDataPreprocessor()

	// processes training data and puts it into one collection, with proper label (0,1)
	trainingDataPreprocessor.process(labeledTrainingFile,unlabeledTrainingFile, trainingCollection,dbName,stopWordsFile)
	
	// init map with all words in collection
	vectorizer.initAllWordsMap(mongoConn, dbName, trainingCollection)

	val trainingTweets = mongoConn(dbName)(trainingCollection)
	
	println("[INFO] creating vectors from training tweets contents")

	trainingTweets.foreach { t =>
		var content = parseJSON(t("content").toString).map(_.toString).toArray
		val vector = vectorizer.createVectorForContent(content,t("label").asInstanceOf[Int])
		t("content") = vector
		trainingTweets.save(t)
	}

	println("[SUCCESS] created vectors for training tweets")

	println("[INFO] creating vectors from tweets ")

	val tweets = mongoConn(dbName)(collectionName)
	val tweetsVectors = mongoConn(dbName)(vectorCollection)

	tweets.foreach { t =>
		var content = parseJSON(t("content").toString).map(_.toString).toArray
		val vector = vectorizer.createVectorForContent(content,0) //label is bogus
		t("content") = vector.slice(0,vector.length-1) //no label this time
		tweetsVectors.save(t)
	}

	println("[SUCCESS] created vectors for tweets")


	println("[INFO] Retrieving data as RDD for training and predicting")

	val trainingRDD = sc.newAPIHadoopRDD(mongoTrainingConfig,classOf[MongoInputFormat],classOf[Object],classOf[BSONObject])

	val trainingData = trainingRDD.map { bson =>
		val vector = bson._2.get("content").asInstanceOf[BasicDBList].toArray
		LabeledPoint(vector(vector.length-1).asInstanceOf[Int].toDouble,Vectors.dense(vector.slice(0,vector.length-1).map(_.asInstanceOf[Int].toDouble)))
	}

	val numIterations = 40

	println("[INFO] Training model")

	val model = SVMWithSGD.train(trainingData, numIterations)

	println("[SUCCESS] Model successfully trained")
	println("[INFO] Retrieving tweets as RDD")

	val tweetsRDD = sc.newAPIHadoopRDD(mongoTweetsConfig,classOf[MongoInputFormat],classOf[Object],classOf[BSONObject])

	println("[SUCCESS] Retrieved RDD")
	println("[INFO] Predicting values")

	val labelsRDD = tweetsRDD.map { bson =>
		val features = bson._2.get("content").asInstanceOf[BasicDBList].toArray
		val label = model.predict(Vectors.dense(features.map(_.asInstanceOf[Int].toDouble)))
		val labeledTweet = new BasicBSONObject()
		labeledTweet.put("tweetid", bson._2.get("tweetid"))
		labeledTweet.put("label",label)
		(null, labeledTweet)
	}

	println("[SUCCESS] Completed prediction, proceeding to saving")

	//must be in this format
	/*val saveRDD = trainingData.map { t =>
		val bson = new BasicBSONObject()
		bson.put("sample","sample_content")
		(null,bson)
	}*/


	labelsRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[MongoOutputFormat[Any, Any]], mongoTweetsConfig);

	println("[SUCCESS] Finished")

	sc.stop()

}