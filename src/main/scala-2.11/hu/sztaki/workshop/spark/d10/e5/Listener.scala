package hu.sztaki.workshop.spark.d10.e5

import org.apache.spark.scheduler.{SparkListenerExecutorMetricsUpdate, SparkListener}

/**
  * @todo[18] Create a custom SparkListener that prints out the result size
  *           when a metrics arrives from the executor.
  *           Print out the shuffle bytes written.
  *           We do it together.
  */
class Listener extends SparkListener{
  override def
  onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate):
  Unit = {
    executorMetricsUpdate.taskMetrics.foreach { metrics =>
      println(s"Result size is: ${metrics._4.shuffleWriteMetrics.map { _.shuffleBytesWritten }}")
    }
  }
}
