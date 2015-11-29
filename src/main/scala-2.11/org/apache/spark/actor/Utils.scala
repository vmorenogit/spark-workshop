package org.apache.spark.actor

import org.apache.spark.{SparkConf, SecurityManager}
import org.apache.spark.util.AkkaUtils

object Utils {
  def get = AkkaUtils
  def security(conf: SparkConf) = new SecurityManager(conf)
}
