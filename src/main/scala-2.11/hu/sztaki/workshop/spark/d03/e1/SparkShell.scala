package hu.sztaki.workshop.spark.d03.e1

import org.apache.spark.{SparkConf, SparkContext}

object SparkShell {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Shell"))

    val data = 1 to 1000

    val distData = sc.parallelize(data)

    val filteredData = distData.filter(_ < 10)

    val output = filteredData.collect()

  }
}