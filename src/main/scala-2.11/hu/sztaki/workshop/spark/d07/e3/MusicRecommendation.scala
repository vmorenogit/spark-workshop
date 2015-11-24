package hu.sztaki.workshop.spark.d07.e3

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

    // 1. Load user artist data
    val rawUserArtistData =
      sc.textFile(userArtistDataFile)

    // 2. Explore user and artist data

    // 3. Load the artist ids and names to an RDD[(Int, String)] from artist data
    val rawArtistData =
      sc.textFile(artistDataFile)

    rawArtistData.take(10).foreach(println)

    val artistData = rawArtistData.map { line =>
      val (id, name) = line.span(_ != '\t')
      (id.toInt, name.trim)
    }

    artistData.collect().foreach(println)

    // 4. Some lines are corrupted, they cause a NumberFormatException
    // Avoid these lines. (Using Option: Some / None)

    // 5. Some artist names are misspelled.
    // There are artist aliases to overcome this.
    // Create a simple map
    // of "bad" artist ids to "good" artist ids
    val rawArtistAlias = null

    // 6. Broadcast the artist aliases.
    // Create a rating with using the good artist id.
    // A user rate should be the number it listened to an artist.

    // 7. Build an ALS model.
    // Use the following parameters:
    // rank = 10, iterations = 5,
    // lambda = 0.01, alpha = 1.0

    // 8. Print a user feature vector.

    // 9. Check what user number 2038659 listens to
    // Print artist names.

    // 10. Make five recommendations to the same user.
    // Print artist names.

    // 11. Split data to training and evaluation data.
    // Use Utils.areUnderCurve to mease the performance of ALS

    // 12. Implement predictMostListened function and compare it to the ALS recommendation.

    // 13. Do evaluations on setting the hyperparamters of the algorithm

    // 14. Give a recommendation to user 2038659 again with the best model according to the evaluation
  }

  def predictMostListened(
                           sc: SparkContext,
                           train: RDD[Rating])(allData: RDD[(Int,Int)]): RDD[Rating] = {
    // Do rating based on the training data.
    // A user should rate the artist 0.0 if its not
    // in the training data set, otherwise the rate
    // should be the total listens to that artist.
    null
  }

}
