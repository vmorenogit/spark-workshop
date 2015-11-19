package hu.sztaki.workshop.spark.d04.e2

import org.apache.spark.{SparkConf, SparkContext}

object KMeansImplementation {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("KMeans implementation")
      .setMaster("local[1]"))

    // Load and parse the data
    // Create dense Vectors of Double

    // Print the RDD to see it working

    // implement KMeans (take RDD, numClusters, numIterations)

    val numOfCentroids = 2
    val numOfIterations = 10
    
    val centroids = null

    // compute the squared error of the model

    // Now instead of the own implementation
    // use MLlib (KMeans.train) to create the centroids

    // Check the squared error on your data (KMeansModel.computCost)
  }

}