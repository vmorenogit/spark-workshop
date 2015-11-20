package hu.sztaki.workshop.spark.d04.e2

import org.apache.spark.rdd.RDD

object KMeansAlgorithm extends Serializable {

  def addPoints(v1: Array[Double], v2: Array[Double]): Array[Double] = {
    // implement adding two points
    ???
  }

  def dist(v1: Array[Double], v2: Array[Double]): Array[Double] = {
    // implement distance between two points
    ???
  }

  def nearestCentroid(centroids: Array[Centroid], p: Array[Double]): (Centroid, Array[Double]) = {
    // implement getting the nearest centroid to a point
    ???
  }

}

class KMeansAlgorithm(data: RDD[Array[Double]], k: Int, numIterations: Int) {

  private def initWithRandomSample(): Array[Centroid] = {
    // Choose k points from the data randomly to create centroids
    data
      .takeSample(withReplacement = false, k)
      .zipWithIndex
      .map { case (point, idx) => Centroid(idx, point) }
  }

  def run(): Array[Centroid] = {

    // Get the SparkContext from the data RDD
    val sc = data.context

    // Create initial centroids with initWithRandomSample
    val initCentroids = initWithRandomSample()

    initCentroids.foreach(p => println(p))

    // Create bcCentroids broadcast variable
    // (You can make it var)

    var i = 0
    while (i < numIterations) {

      val newCentroids = null // create the new centroids
        // 1. find nearest centroid for every point
        // 2. map to (centroid_index, (point, 1)) pairs
        // 3. compute the sum and count of points in one reduceByKey for the average
        // 4. count the average for each centroid: sum / count

      // broadcast the new centroids (use bcCentroids var again)

      i += 1
    }

    // return the finally got centroids
    ???
  }

}

case class Centroid(idx: Int, point: Array[Double]) {
  override def toString: String = idx + ": " + point.mkString(" ")
}