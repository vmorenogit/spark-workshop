package hu.sztaki.workshop.spark.d04.e2

import org.apache.spark.rdd.RDD

object KMeansAlgorithm extends Serializable {

  def addPoints(v1: Array[Double], v2: Array[Double]): Array[Double] = {
    // implement adding two points
    val zipped = v1.zip(v2)
    val sum = zipped.map { case (e1, e2) => e1 + e2 }
    sum
  }

  def dist(v1: Array[Double], v2: Array[Double]): Double = {
    // implement distance between two points
    val sqDist = v1.zip(v2).map { case (e1, e2) => (e1 - e2)*(e1 - e2)}.sum
    Math.sqrt(sqDist)
  }

  def nearestCentroid(centroids: Array[Centroid], p: Array[Double]): (Centroid, Array[Double]) = {
    // implement getting the nearest centroid to a point
    val nearestCentroid = centroids.minBy(c => dist(p, c.point))
    (nearestCentroid, p)
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
    var bcCentroids = sc.broadcast(initCentroids)

    var i = 0
    while (i < numIterations) {

      val newCentroids = data // create the new centroids
        // 1. find nearest centroid for every point
        .map(p => KMeansAlgorithm.nearestCentroid(bcCentroids.value, p))
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