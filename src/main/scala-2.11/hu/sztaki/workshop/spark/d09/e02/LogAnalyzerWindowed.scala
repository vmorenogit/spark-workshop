package hu.sztaki.workshop.spark.d09.e02

import hu.sztaki.workshop.hadoop.d02.ApacheAccessLog
import hu.sztaki.workshop.spark.d03.e3.AdvancedRDD.RichRDD
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}

/**
 * Computes various pieces of information on a sliding window form the log input.
 */
object LogAnalyzerWindowed {
  def responseCodeCount(accessLogRDD: RDD[ApacheAccessLog]) = {
    /**
      * @todo[17] Wordcount all over again? Use `RichRDD`.
      * @hint ;)
      */
  }

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog], opts: Config) {
    /**
      * @todo[12] Count the distinct IP addresses in a sliding window:
      * @hint Get the window size and slide duration from parsed options.
      */

    /**
      * @todo[13] Save the result as a text-file.
      * @hint Get the output directory from the options.
      */

    /**
      * @todo[14] Save the result as a hadoop file.
      * @hint Get the output directory from the options.
      * @hint For that, you need to transform the IP and count pair into Hadoop's types.
      *       What would be the two types?
      */


    /**
      * @todo[15] Count the total access logs in a sliding window.
      */


    /**
      * @todo[16] Print out the result of [15] and [14].
      */


    /**
      * @todo[16] Redefine `accessLogsDStream` as a new windowed DStream.
      * @hint Use window duration and slide duration from options.
      */


    /**
      * @todo[17] Count the response codes.
      *           Use transform method with `responseCodeCount`.
      *           Print them also.
      */


    /**
      * @todo[18] Compute the visit counts for IP address in a window.
      *           Use DStream.`reduceByKeyAndWindow`.
      *           Print out the results.
      */
  }
}