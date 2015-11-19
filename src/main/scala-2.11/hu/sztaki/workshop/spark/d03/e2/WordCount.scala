package hu.sztaki.workshop.spark.d03.e2

import org.apache.spark._

object Wordcount {
  def main(args: Array[String]) {
    val inputFile = "src/test/test_data/hamlet.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.
    val counts = input
      .flatMap(line => line.split(" "))
      .filter(_.contains("a"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .take(10)

    // zoltan.zvara@gmail.com
    // marton.balassi@gmail.com
    // Save the word count back out to a text file, causing evaluation.
    counts foreach { println }
  }
}