package hu.sztaki.workshop.spark.d04.e1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

object VectorsIntro {

  def main(args: Array[String]) {

    //    Use Vectors.dense to create a Vector with these values:
    //    [1.0, 0.0, 0.0, 0.0, 2.5, 0.0]
    val denseVec1 = Vectors.dense(1.0, 0.0, 0.0, 0.0, 2.5, 0.0)
    val denseVec2 = Vectors.dense(Array(1.0, 0.0, 0.0, 0.0, 2.5, 0.0))

    //    Compress it using Vector.toSparse
    val sparseVec1 = denseVec1.toSparse

    //    How good is the compression? (See Vector.numActives and Vector.numNonzeros)
    println(denseVec1.numActives)
    println(denseVec1.numNonzeros)

    println()

    println(sparseVec1.numActives)
    println(sparseVec1.numNonzeros)

    //    Specify the same vector with Vectors.sparse
    val sparseVec2 = Vectors.sparse(6, Array((1, 2.0), (3, 4.0)))

  }

}