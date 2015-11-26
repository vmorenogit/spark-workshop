package hu.sztaki.workshop.spark.d09.e02

import hu.sztaki.workshop.hadoop.d02.ApacheAccessLog
import org.apache.spark._
import org.apache.spark.streaming._

/**
 * The LogAnalyzerAppMain is an sample logs analysis application.  For now,
 * it is a simple minimal viable product:
 *   - Read in new log files from a directory and input those new files into streaming.
 *   - Computes stats for all of time as well as the last time interval based on those logs.
 *   - Write the calculated stats to an text file on the local file system
 *     that gets refreshed every time interval.
 *
 * Once you get this program up and running, feed apache access log files
 * into the local directory of your choosing.
 *
 * Then open your output text file, perhaps in a web browser, and refresh
 * that page to see more stats come in.
 *
 * Modify the command line flags to the values of your choosing.
 * Notice how they come after you specify the jar when using spark-submit.
 */
case class Config(WindowLength: Int = 3000,
                  SlideInterval: Int = 6000,
                  LogsDirectory: String = "/tmp/logs",
                  CheckpointDirectory: String = "/tmp/checkpoint",
                  OutputHTMLFile: String = "/tmp/log_stats.html",
                  OutputDirectory: String = "/tmp/outpandas",
                  IndexHTMLTemplate :String ="./src/main/resources/index.html.template") {
  def windowDuration = {
    new Duration(WindowLength)
  }
  def slideDuration = {
    new Duration(SlideInterval)
  }
}

object LogAnalyzerAppMain {
  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("LogAnalyzerAppMain") {
      head("LogAnalyzer", "0.1")
      opt[Int]('w', "window_length") text "size of the window as an integer in miliseconds"
      opt[Int]('s', "slide_interval") text "size of the slide inteval as an integer in miliseconds"
      opt[String]('l', "logs_directory") text "location of the logs directory.."
      opt[String]('c', "checkpoint_directory") text "location of the checkpoint directory."
      opt[String]('o', "output_directory") text "location of the output directory."
    }
    /**
      * @todo[1] Use the parser to parse configuration.
      */
    val opts = parser.parse(args, new Config()).get

    /**
      * @todo[2] Create the Spark configuration.
      */
    val conf = new SparkConf()
      .setAppName("LogAnalyzer great app")
      .setMaster("local")

    /**
      * @todo[3] Create Streaming context.
      */
    val ssc = new StreamingContext(conf, opts.windowDuration)

    /**
      * @todo[4] Configure checkpointing (read from options, checkpoint directory).
      */
    ssc.checkpoint(opts.CheckpointDirectory)

    /**
      * @todo[5] Monitor the directory for new files.
      * @hint You need to create some sort of DStream. Which one?
      */
    val logData = ssc.textFileStream(opts.LogsDirectory)

    /**
      * @todo[6] Transform each log line into an ApacheAccessLog.
      * @hint Look at the static methods of ApacheAccessLog (Java) class.
      */
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line)).cache()

    /**
      * @todo Complete function!
      */
    LogAnalyzerTotal.processAccessLogs(accessLogDStream)

    /**
      * @todo Complete function!
      */
    // LogAnalyzerWindowed.processAccessLogs(accessLogDStream, opts)

    ssc.start()
    ssc.awaitTermination()
  }
}