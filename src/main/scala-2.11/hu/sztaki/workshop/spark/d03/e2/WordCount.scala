package hu.sztaki.workshop.spark.d03.e2

import org.apache.spark._

object Wordcount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("WordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.
    val counts = input
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    // Save the word count back out to a text file, causing evaluation.
    counts foreach { println }
  }
}