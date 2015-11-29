package hu.sztaki.workshop.spark.d04.e2

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansImplementation {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("KMeans implementation")
      .setMaster("local[1]"))

    // Load and parse the data
    // Create dense Vectors of Double
    val data = sc.textFile(args(0))

    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Print the RDD to see it working
    parsedData.collect().foreach(println)

    // implement KMeans (take RDD, numClusters, numIterations)

    val numOfCentroids = 2
    val numOfIterations = 10
    
    val centroids =
      new KMeansAlgorithm(parsedData, numOfCentroids, numOfIterations).run()

    // compute the squared error of the model
    val sqError = parsedData
      .map(p => KMeansAlgorithm.nearestCentroid(centroids, p))
      .map { case (c, p) => Vectors.sqdist(c.vec, p)}
      .sum()

    println("Own implementation centroids:")
    centroids.foreach(println)
    println("Squared error: " + sqError)

    println()

    // Use MLlib (KMeans.train) to create the centroids
    val model = KMeans.train(parsedData, numOfCentroids, numOfIterations)

    // Check the squared error on your data (KMeansModel.computCost)
    println("MLlib centroids:")
    model.clusterCenters.foreach(println)
    println("Squared error: " + model.computeCost(parsedData))
  }

}