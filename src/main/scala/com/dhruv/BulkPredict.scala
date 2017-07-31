package com.dhruv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayesModel
//import org.apache.spark.streaming.twitter._
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Pulls live tweets and predicts the sentiment.
 */
object BulkPredict {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <modelDirectory> ")
      System.exit(1)
    }

    val Array(modelFile) =
      Utils.parseCommandLineWithTwitterCredentials(args)

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    //val ssc = new StreamingContext(conf, Seconds(5))

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    //println("Initializing Twitter stream...")
    //val tweets = TwitterUtils.createStream(ssc, Utils.getAuth)
    //val statuses = tweets.filter(_.getLang == "en").map(_.getText)
    println("Getting bulk data ...")
    val allData = sc.textFile(args(0))
    val header = allData.first()
    val statuses = allData.filter(x => x != header)

    println("Initalizaing the Naive Bayes model...")
    val model = NaiveBayesModel.load(sc, modelFile.toString)

    def toLabels(line: String) = {
      val words = line.split(',')
      (words(0), words(1), words(2))
    }

    val labeled_statuses = statuses.map(x => toLabels(x))
      .map(t => (t._1, t._2, t._3, model.predict(Utils.featurize(t._3))))
      .toDF("date", "bank", "tweet", "sentiment-analysis")

    //labeled_statuses.print()

    labeled_statuses.write.saveAsTable("sentiment")

    // Start the streaming computation
    //println("Initialization complete.")
    //ssc.start()
    //ssc.awaitTermination()
    sc.stop()
  }
}
