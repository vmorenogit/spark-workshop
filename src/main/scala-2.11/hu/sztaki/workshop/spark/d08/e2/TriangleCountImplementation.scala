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

    // Load graph with GraphLoader.edgeListFile

    // Create undirected graph where edges are like src < dst

    // Send from src to dst all neighbor that satisfies neighbor < src < dst

    // Intersect the got vertexIds with the neighbors. You get the triangle count :)
  }
}
