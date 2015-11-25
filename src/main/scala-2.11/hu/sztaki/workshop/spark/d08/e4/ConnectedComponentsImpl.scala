package hu.sztaki.workshop.spark.d08.e4

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponentsImpl {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Connected components GraphX")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val edgeFile = args(0)
    val vertexFile = args(1)

    // 1. Load graph with GraphLoader.edgeListFile
    val graph = GraphLoader.edgeListFile(sc, edgeFile)

    // 2. Let the vertex attribute be
    // the vertexId itself
    val ccGraph = graph.mapVertices { case (vid, _) => vid }

    // 3. Create a function that sends
    // a vertex attribute message along
    // an edge: from the vertex with the higher
    // attribute to the one with lower.
    def sendMessage(edge: EdgeTriplet[VertexId, Int]):
      Iterator[(VertexId, VertexId)] = null

    // 4. Use pregel with this send message to
    // propagate maximum value iteratively.
  }
}
