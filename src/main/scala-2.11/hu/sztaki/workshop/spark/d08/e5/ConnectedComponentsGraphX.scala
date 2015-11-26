package hu.sztaki.workshop.spark.d08.e5

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponentsGraphX {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Connected components GraphX")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val edgeFile = args(0)
    val vertexFile = args(1)

    // 1. Load graph with GraphLoader.edgeListFile

    // 2. Find the connected components

    // 3. Join the connected components with the usernames

    // 4. Print the result
  }
}
