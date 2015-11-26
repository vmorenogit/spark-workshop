package hu.sztaki.workshop.spark.d08.e3

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object TriangleCountingGraphX {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Triangle counting GraphX")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val edgeFile = args(0)
    val vertexFile = args(1)

    // 1. Load the edges in canonical order
    // and partition the graph for triangle count

    // 2. Find the triangle count for each vertex
    // (usig GraphX)

    // 3. Join the triangle counts with the usernames

    // 4. Print the result

    // 5. Get the total triangle count
  }
}
