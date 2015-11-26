package hu.sztaki.workshop.spark.d09.e02

import hu.sztaki.workshop.hadoop.d02.ApacheAccessLog
import org.apache.spark.streaming.dstream._

/**
 * @todo Compute totals on the log input.
 */
object LogAnalyzerTotal {
  def computeRunningSum(values: Seq[Long], state: Option[Long]): Option[Long] = {
    /**
      * @todo[11] Update state, which is a long value.
      * @hint Learn about Option.
      * @hint Return with the updated state (Some).
      */
    Some(1L)
  }

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog]) {
    /**
      * @todo[7] Count the frequency for each IP address.
      */
    val ipDStream = accessLogsDStream
      .map(entry => (entry.getIpAddress, 1))
      .reduceByKey(_ + _)
      .print()

    /**
      * @todo[8] Do the same:
      *          Count the frequency for each IP address,
      *          but by using DStream.`transform` method.
      */

    /**
      * @todo[9] Calculate the bytes transfered by IP address,
      *          but also show the number of transfers per IP address.
      *          The output should be: ({IP}, ({bytes}, {number_of_transfers}))
      */

    /**
      * @todo[10] Compute a running sum for each response code!
      * @hint Map to a key-value pair, then use DStream.`updateStateByKey` with the
      *       `computeRunningSum` method of this class
      */

  }
}