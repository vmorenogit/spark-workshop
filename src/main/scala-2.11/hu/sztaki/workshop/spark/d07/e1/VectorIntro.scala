package hu.sztaki.workshop.spark.d07.e1

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

object VectorIntro {

  def main(args: Array[String]) {

    //    Use Vectors.dense to create a Vector with these values:
    //    [1.0, 0.0, 0.0, 0.0, 2.5, 0.0]
    val d1 = Vectors.dense(1.0, 0.0, 0.0, 0.0, 2.5, 0.0)

    println(d1)

    //    Compress it using Vector.toSparse
    val s1 = d1.toSparse

    println(s1)

    //    How good is the compression? (See Vector.numActives and Vector.numNonzeros)
    println(d1.numActives + " nz: " + d1.numNonzeros)

    println(s1.numActives + " nz: " + s1.numNonzeros)

    //    Specify the same vector with Vectors.sparse
    val s2 = Vectors.sparse(6, Array(0, 4), Array(1.0, 2.5))

    println(s2)
  }

}