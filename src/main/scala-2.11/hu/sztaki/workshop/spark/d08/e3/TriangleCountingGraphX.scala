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

    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, edgeFile, true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices

    // Join the triangle counts with the usernames
    val users = sc.textFile(vertexFile).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }

    // Print the result
    println(triCountByUsername.collect().mkString("\n"))

    // Get the total triangle count
    println(s"total triangles: ${triCounts.values.sum() / 3}")
  }
}
