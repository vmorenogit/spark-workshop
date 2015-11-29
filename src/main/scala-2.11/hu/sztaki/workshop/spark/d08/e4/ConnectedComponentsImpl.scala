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

    // Load graph with GraphLoader.edgeListFile
    val graph = GraphLoader.edgeListFile(sc, edgeFile)

    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, Int]): Iterator[(VertexId, VertexId)] = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }

    val initialMessage = Long.MaxValue

    val cc = Pregel(ccGraph, initialMessage, activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))

    cc.vertices.collect().foreach(println)

  }
}
