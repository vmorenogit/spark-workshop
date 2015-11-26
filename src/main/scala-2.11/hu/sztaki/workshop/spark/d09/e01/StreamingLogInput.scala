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

    /**
      * @todo[2] Create StreamingContext with a batch duration of 1 second.
      */

    /**
      * @todo[2] Create a DStream from all the input on port 7777.
      * @hint You need to create a socket listener.
      */

    /**
      * @todo[4] Print out the DStream.
      */

    /**
      * @todo[5] Start the streaming context and wait for it to finish.
      */

    /**
      * @todo[6] Wait for termination: 60 seconds.
      */

    /**
      * @todo[7] Stop streaming context.
      */

  }
  def processLines(lines: DStream[String]) = {
    /**
      * @todo[3] Filter our DStream for lines with "error".
      */
  }
}