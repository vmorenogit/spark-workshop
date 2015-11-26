package hu.sztaki.workshop.spark.d09.e02

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

/**
 * Illustrates reading in transfer statistics.
 * @todo[19] What happens here? Also, try to run it!
 */
object ReadTransferStats {
  def readStats(ssc: StreamingContext, inputDirectory: String): DStream[(Long, Int)] = {
    // Convert the input from Writables to native types.
    ssc.fileStream[LongWritable, IntWritable,
      SequenceFileInputFormat[LongWritable, IntWritable]](inputDirectory).map {
      case (x, y) => (x.get(), y.get())
    }
  }
}