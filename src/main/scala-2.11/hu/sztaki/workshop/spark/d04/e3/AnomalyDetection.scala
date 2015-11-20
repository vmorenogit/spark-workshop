package hu.sztaki.workshop.spark.d04.e3

import hu.sztaki.workshop.spark.d04.e2.{KMeansAlgorithm, Centroid}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AnomalyDetection {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("Anomaly detection")
      .setMaster("local[2]"))

    // 1.
    // Explore data. Take some lines from the CSV file.
    val lines = sc.textFile(args(0))

    lines.take(10).foreach(println)

    // 2.
    // Connections are labeled. How many are there of labels (e.g. there are 972781 'normal')?
    val lbs = lines.map(_.split(",").last).countByValue()
    lbs.toSeq.sortBy(_._2).reverse.foreach(println)

    // 3.
    // Some features are nonnumeric (categorical) remove them from the parsed data.
    // Parse the other features as numeric data (Array[Double][Double]).

    val data = null

    // Build a KMeans model with 10 centroids.
    // Take a look at the centroids.
    //
    // (It takes a while to build the model.)

    // How many clusters are there? See how it fits to the labels.

    // Define two point's Euclidean distance
    // distToCentroid: distance of a Array[Double] to the nearest centroid based on a KMeansModel
    // clusteringScore: average distance to the nearest centroid based on a model built with k

    // Print the cost of the model for k = 5, 10, 15 ... 40

    // Improve KMeans model building by running it multiple times and more precisely.
    // Run with k = 30, 40 ... 100

    // Standardize the data and run again with k = 60, 70 .. 120
    val standardizer: Array[Double] => Array[Double] = buildNormalizationFunction(data)

    // Use an ideal k to build the model on the whole data set and find anomalies in the
    // original data.
    // First define a threshold distance to centroid as the 100th furthest point from its centroid.
  }

  def distance(a: Array[Double], b: Array[Double]) = // define distance
    null

  def distToCentroid(datum: Array[Double], model: Array[Centroid]): Double = {
    // get the distance to the nearest centroid based on the model
    0.0
  }

  def clusteringScore(data: RDD[Array[Double]], k: Int): Double = {
    ???
  }

  def buildNormalizationFunction(data: RDD[Array[Double]]): (Array[Double] => Array[Double]) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
        (a, b) => a.zip(b).map(t => t._1 + t._2)
      )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Array[Double]) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
      )
      normalizedArray
    }
  }

}
