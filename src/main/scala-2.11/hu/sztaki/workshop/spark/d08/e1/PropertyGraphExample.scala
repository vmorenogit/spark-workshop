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
    val vertices: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (1L, ("apple", "sweet")),
        (2L, ("pear", "sweet")),
        (3L, ("banana", "tasty")),
        (4L, ("anna", "person")),
        (5L, ("george", "person"))
      )
      )

    // RDD of edges
    val edges: RDD[Edge[String]] = sc.parallelize(
      Array(
        Edge(1L, 2L, "similar to"),
        Edge(4L, 2L, "eats"),
        Edge(5L, 3L, "likes"),
        Edge(5L, 4L, "is friend of")
      )
    )

    // Build graph
    val graph = Graph(vertices, edges)

    // Count the number of persons
    val persons = graph.vertices.filter{
      case (id, (name, prop)) =>
        prop == "person"
    }

    persons.collect().foreach(println)

    // Count the edges where src > dst
    val cnt = graph.edges.filter(e => e.srcId > e.dstId)
      .count()

    println(cnt)

    // Print the edges in
    // readable form (i.e. names,
    // vertex attributes, edge attributes).
    // Use Graph.triplets.
    val nice = graph.triplets.map { tr =>
      tr.srcAttr + " " + tr.attr + " " + tr.dstAttr
    }

    nice.collect().foreach(println)


    // Get the out degree of everything.
    // Use Graph.outDegrees and Graph.outerJoinVertices
  }

}
