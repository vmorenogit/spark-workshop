package hu.sztaki.workshop.spark.d04.e2

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object KMeansAlgorithm extends Serializable {

  // this is not efficient
  def addVectors(v1: Vector, v2: Vector): Vector = {
    Vectors.dense(v1.toArray.zip(v2.toArray).map { case (x, y) => x + y })
  }

  // this is not efficient
  def mapVector(f: Double => Double, v: Vector): Vector = {
    Vectors.dense(v.toArray.map(f))
  }

  def nearestCentroid(centroids: Array[Centroid], v: Vector): (Centroid, Vector) = {
    val nearestCentroid = centroids.minBy(c => Vectors.sqdist(c.vec, v))
    (nearestCentroid, v)
  }

}

class KMeansAlgorithm(data: RDD[Vector], k: Int, numIterations: Int) {

  private def initWithRandomSample(): Array[Centroid] = {
    // Choose k points from the data randomly to create centroids
    data
      .takeSample(withReplacement = false, k)
      .zipWithIndex
      .map { case (vec, idx) => Centroid(idx, vec) }
  }

  def run(): Array[Centroid] = {

    val sc = data.context

    val initCentroids = initWithRandomSample()

    initCentroids.foreach(println)

    var bcCentroids = sc.broadcast(initCentroids)

    var i = 0
    while (i < numIterations) {

      val newCentroids = data
        // find nearest centroid
        .map(p => KMeansAlgorithm.nearestCentroid(bcCentroids.value, p))
        // map to (centroid_index, (point, 1)) pairs
        .map { case (c, p) => (c.idx, (p, 1L)) }
        // compute the sum and count of points in one reduceByKey for the average
        .reduceByKey { case ((p1, cnt1), (p2, cnt2)) =>
          (KMeansAlgorithm.addVectors(p1, p2), cnt1 + cnt2)
        }
        // count the average for each centroid: sum / count
        .map { case (cIdx, (sumVec, cnt)) =>
          Centroid(cIdx, KMeansAlgorithm.mapVector(x => x / cnt, sumVec))
        }

      // broadcast the new centroids
      bcCentroids = sc.broadcast(newCentroids.collect())

      i += 1
    }

    bcCentroids.value
  }

}

case class Centroid(idx: Int, vec: Vector)
