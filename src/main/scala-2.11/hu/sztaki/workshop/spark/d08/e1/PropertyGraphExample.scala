package hu.sztaki.workshop.spark.d08.e1

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PropertyGraphExample {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Property graph example")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    /* Construct a simple property graph with
     vertices:
      apple   sweet
      pear    sweet
      banana  tasty
      anna    person
      george  person

     and edges:
      apple   similar to      pear
      anna    eats            pear
      george  likes           banana
      george  is friend of    anna
    */

    // RDD of vertices
    val users: RDD[(VertexId, (String, String))] =
      null

    // RDD of edges
    val relationships: RDD[Edge[String]] =
      null

    // Build graph

    // Count the number of persons

    // Count the edges where src > dst

    // Print the edges in readable form (i.e. names, vertex attributes, edge attributes).
    // Use Graph.triplets.

    // Get the out degree of everything.
    // Use Graph.outDegrees and Graph.outerJoinVertices
  }

}
