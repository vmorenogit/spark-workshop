package hu.sztaki.workshop.spark.d04.e2

import org.apache.spark.{SparkConf, SparkContext}

object KMeansImplementation {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("KMeans implementation")
      .setMaster("local[1]"))

    // Load and parse the data
    val rawData = sc.textFile(args(0))

    val data = rawData.map(_.split(' ').map(_.toDouble))

    // Print the RDD to see it working
    data.collect().foreach(arr => println(arr.mkString(",")))

    // implement KMeans (take RDD, numClusters, numIterations)

    val numOfCentroids = 2
    val numOfIterations = 10

    val kmeans = new KMeansAlgorithm(data, numOfCentroids, numOfIterations)
    val centroids = kmeans.run()

    // compute the squared error of the model
  }

}