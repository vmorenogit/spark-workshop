package hu.sztaki.workshop.spark.d09.e01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Wordcount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming wordcount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)

    lines
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination(60000)
    ssc.stop()

  }
}