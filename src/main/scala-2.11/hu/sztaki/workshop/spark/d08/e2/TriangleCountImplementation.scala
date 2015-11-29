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
    val graph = GraphLoader.edgeListFile(sc, edgeFile, canonicalOrientation = true)

    // Create undirected graph where edges are like src < dst
    val undirEdges = graph.edges.union(graph.edges.reverse).distinct()

    val undirGraph = Graph.fromEdges(undirEdges, defaultValue = 0)

    val neighbours = undirGraph.collectNeighbors(EdgeDirection.Out)

    val graphWithNeighborAttr = undirGraph
      .mapVertices { case (vId, v) => Array[Long]() }
      .joinVertices(neighbours) { case (vId, v, neigh) =>
      neigh.map { case (neighId, _) => neighId }
    }

    graphWithNeighborAttr.vertices.collect()
      .foreach(x => println(x._1 + ": " + x._2.mkString(" ")))
    println()

    // Send from src to dst all neighbor that satisfies neighbor < src < dst
    val sndNeighbors = graphWithNeighborAttr.aggregateMessages[Array[Long]](
      triplet => {
        if (triplet.srcId < triplet.dstId) {
          val smallerNeighs = triplet.srcAttr.filter(_ < triplet.srcId)
          triplet.sendToDst(smallerNeighs)
        }
      }
      , (s1, s2) => s1 ++ s2
    )

    val triCount = graphWithNeighborAttr.outerJoinVertices(sndNeighbors) {
      case (vId, neighs, sndNeighs) =>
        sndNeighs match {
          case Some(ns) => neighs.intersect(ns).length
          case None => 0
        }
    }
      .vertices.values.sum()

    println(s"num of triangles: ${triCount}")
  }
}
