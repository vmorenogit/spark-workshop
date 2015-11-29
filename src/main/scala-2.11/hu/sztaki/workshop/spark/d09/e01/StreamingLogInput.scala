package hu.sztaki.workshop.spark.d09.e01

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object StreamingLogInput {
  def main(args: Array[String]) {
    /**
      * @todo[1] Create SparkConf.
      */
    val conf = new SparkConf().setAppName("StreamingLogInput")
    /**
      * @todo[2] Create StreamingContext with a batch duration of 1 second.
      */
    val ssc = new StreamingContext(conf, Seconds(1))
    /**
      * @todo[2] Create a DStream from all the input on port 7777.
      * @hint You need to create a socket listener.
      */
    val lines = ssc.socketTextStream("localhost", 7777)
    val errorLines = processLines(lines)

    /**
      * @todo[4] Print out the DStream.
      */
    errorLines.print()

    /**
      * @todo[5] Start the streaming context and wait for it to finish.
      */
    ssc.start()

    /**
      * @todo[6] Wait for termination: 60 seconds.
      */
    ssc.awaitTermination(60000)

    /**
      * @todo[7] Stop streaming context.
      */
    ssc.stop()
  }
  def processLines(lines: DStream[String]) = {
    /**
      * @todo[3] Filter our DStream for lines with "error".
      */
    lines.filter(_.contains("error"))
  }
}