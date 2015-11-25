package hu.sztaki.workshop.spark.d08.e6

import org.apache.spark.{SparkContext, SparkConf}

object FacebookGraph {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Facebook graph")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val edgeFile = args(0)
    val vertexFile = args(1)

    // 1. Load facebook_combined.txt graph

    // 2. Get the number of edges

    // 3. Get the number of vertices

    // 4. Get the number of triangles

    // 5. Get the number of vertices in the
    // largest connected component

    // (*) 6. Get the number of edges in the
    // largest connected component

    // (*) 7. Get the degree distribution:
    // the number of vertices with the same degrees.
    // Print the ones with the 5 smallest degrees.

    // 8. Use page rank to rank the users.
    // Get the top 5.
  }
}
