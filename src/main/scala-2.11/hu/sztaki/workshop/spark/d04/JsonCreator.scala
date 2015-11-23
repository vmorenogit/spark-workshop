package hu.sztaki.workshop.spark.d04

import org.apache.spark.{SparkConf, SparkContext}

object JsonCreator {

  def main(args: Array[String]) = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("json creator")
        .setMaster("local")
    )

    val inputPath = args(0)
    val outputPath = args(1)

    sc
      .textFile(inputPath)
      .map(line => {
        val v = line.split("\t")
        val timestamp = v(0)
        val site = v(1)
        val requests = v(2)

        "{\"timestamp\": " ++ timestamp ++ ", \"site\": "  ++ site ++ ", \"requests\": " ++
          requests ++ "}";
      })
      .saveAsTextFile(outputPath)

  }
}
