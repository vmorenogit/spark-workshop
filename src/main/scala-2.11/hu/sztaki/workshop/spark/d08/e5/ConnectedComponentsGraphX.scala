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

    // Load graph with GraphLoader.edgeListFile
    val graph = GraphLoader.edgeListFile(sc, edgeFile)

    // Find the connected components
    val cc = graph.connectedComponents().vertices

    // Join the connected components with the usernames
    val users = sc.textFile(vertexFile).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }
}
