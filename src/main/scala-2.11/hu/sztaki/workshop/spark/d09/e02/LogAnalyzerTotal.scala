package hu.sztaki.workshop.spark.d09.e02

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
    Some(values.sum + state.getOrElse(0L))
  }

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog]) {
    /**
      * @todo[7] Count the frequency for each IP address.
      */
    val ipDStream = accessLogsDStream.map(entry => (entry.getIpAddress, 1))
    val ipCountsDStream = ipDStream.reduceByKey((x, y) => x + y)
    ipCountsDStream.print()

    /**
      * @todo[8] Do the same:
      *          Count the frequency for each IP address,
      *          but by using DStream.`transform` method.
      */
    val ipRawDStream = accessLogsDStream.transform{
      rdd => rdd.map(accessLog => (accessLog.getIpAddress, 1)).reduceByKey(
        (x, y) => x + y)
    }
    ipRawDStream.print()

    /**
      * @todo[9] Calculate the bytes transfered by IP address,
      *          but also show the number of transfers per IP address.
      *          The output should be: ({IP}, ({bytes}, {number_of_transfers}))
      */
    val ipBytesDStream = accessLogsDStream.map(entry => (entry.getIpAddress, entry.getContentSize))
    val ipBytesSumDStream = ipBytesDStream.reduceByKey(_ + _)
    val ipBytesRequestCountDStream = ipRawDStream.join(ipBytesSumDStream)
    ipBytesRequestCountDStream.print()

    /**
      * @todo[10] Compute a running sum for each response code!
      * @hint Map to a key-value pair, then use DStream.`updateStateByKey` with the
      *       `computeRunningSum` method of this class
      */
    val responseCodeDStream = accessLogsDStream.map(log => (log.getResponseCode, 1L))
    val responseCodeCountDStream = responseCodeDStream.updateStateByKey(computeRunningSum)
    responseCodeCountDStream.print()
  }
}