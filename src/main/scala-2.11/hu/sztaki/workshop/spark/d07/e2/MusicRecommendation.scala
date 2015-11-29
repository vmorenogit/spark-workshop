package hu.sztaki.workshop.spark.d07.e2

import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MusicRecommendation {

  def main (args: Array[String]) {
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[4]")
      .setAppName("Music recommendation"))

    val userArtistDataFile = args(0)
    val artistDataFile = args(1)
    val artistAliasFile = args(2)

    // Load data into HDFS then
    val rawUserArtistData = sc.textFile(userArtistDataFile)

    // Explore user and artist data
    rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    rawUserArtistData.map(_.split(' ')(1).toDouble).stats()

    // Load the artist ids and names to an RDD[(Int, String)]
    val rawArtistData = sc.textFile(artistDataFile)
    val artistByIDFirstLoad = rawArtistData.map { line =>
      val (id, name) = line.span(_ != '\t')
      (id.toInt, name.trim)
    }

    // Some lines are corrupted, they cause a NumberFormatException
    // Avoid these lines. (Using Option: Some / None)
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

    // Some artist names are misspelled. Create a simple map
    // of "bad" artist ids to "good" artist ids
    val rawArtistAlias = sc.textFile(artistAliasFile)
    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    // Broadcast the artist aliases.
    // Create a rating with using the good artist id.
    // A user rate should be the number it listened to an artist.
    val bArtistAlias = sc.broadcast(artistAlias)

    val allData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID =
        bArtistAlias.value.getOrElse(artistID, artistID)
    Rating(userID, finalArtistID, count)
    }.cache()

    // Build an ALS model.
    val model = ALS.trainImplicit(allData, 10, 5, 0.01, 1.0)

    // Print a user feature vector.
    model.userFeatures.mapValues(_.mkString(", ")).first()

    // Check what user number 2093760 listens to
    // Print artist names.
    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
      filter { case Array(user,_,_) => user.toInt == 2093760 }
    val existingProducts =
      rawArtistsForUser.map { case Array(_,artist,_) => artist.toInt }.
        collect().toSet
    artistByID.filter { case (id, name) =>
      existingProducts.contains(id)
    }.values.collect().foreach(println)

    // Make five recommendations to the same user.
    // Print artist names.
    val recommendations = model.recommendProducts(2093760, 5)
    recommendations.foreach(println)

    val recommendedProductIDs = recommendations.map(_.product).toSet
    artistByID.filter { case (id, name) =>
      recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)

    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()
    val allItemIDs = trainData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)
    val modelForEval = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    val auc = Utils.areaUnderCurve(cvData, bAllItemIDs, modelForEval.predict)

    // Implement predictMostListened function and compare it to the ALS recommendation.
    val aucMostListened = Utils.areaUnderCurve(
      cvData, bAllItemIDs, predictMostListened(sc, trainData))

    // Do evaluations on setting the hyperparamters of the algorithm
    val evaluations =
      for (rank
           <- Array(10, 50);
           lambda <- Array(1.0, 0.0001);
           alpha <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
          val auc = Utils.areaUnderCurve(cvData, bAllItemIDs, model.predict)
          ((rank, lambda, alpha), auc)
        }
    evaluations.sortBy(_._2).reverse.foreach(println)

    // Give a recommendation to user 2093760 again with the best model according to the evaluation
    val bestModel = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    val bestRecommendations = model.recommendProducts(2093760, 5)
    bestRecommendations.foreach(println)

  }

  def predictMostListened(
                           sc: SparkContext,
                           train: RDD[Rating])(allData: RDD[(Int,Int)]) = {
    val bListenCount = sc.broadcast(
      train.map(r => (r.product, r.rating)).
        reduceByKey(_ + _).collectAsMap()
    )
    allData.map { case (user, product) =>
      Rating(
        user,
        product,
        bListenCount.value.getOrElse(product, 0.0)
      )
    }
  }

}
