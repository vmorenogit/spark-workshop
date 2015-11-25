package hu.sztaki.workshop.spark.d08.e2

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object TriangleCountImplementation {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Triangle counting GraphX")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val edgeFile = args(0)

    // 1. Load graph with GraphLoader.edgeListFile

    // 2. Create undirected graph where
    // edges are like src < dst

    // 3. Send from src to dst all neighbor
    // that satisfies neighbor < src < dst

    // 4. Intersect the got vertexIds with
    // the neighbors. You get the triangle count :)
  }
}
