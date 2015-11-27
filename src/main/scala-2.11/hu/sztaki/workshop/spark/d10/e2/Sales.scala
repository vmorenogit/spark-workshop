package hu.sztaki.workshop.spark.d10.e2

import hu.sztaki.workshop.spark.d10.e2.SalesRDDFunctions.addCustomFunctions
import org.apache.spark.{SparkConf, SparkContext}

object Sales {
  def main(args: Array[String]) {
    /**
      * @todo[8] Create Spark context, configuration, read from text file.
      */
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("Sales")
        .setMaster("local")
    )

    val dataRDD = sc.textFile(args(0))

    /**
      * @todo[9] Transform lines to SalesRecord.
      */

    val salesRecordRDD =
      dataRDD.map(line => {
        val values = line.split(",")
        new SalesRecord(values(0), values(1),
          values(2), values(3).toDouble)
      })

    /**
      * @todo[10] Calculate the total sales.
      */
    val totalSales = salesRecordRDD.totalSales
    println(totalSales)

    /**
      * @todo[11] Set up a discount of 0.1 and print out
      *           the new records.
      */
    val discountRDD = salesRecordRDD.discount(0.1)
    discountRDD.collect() foreach println
  }
}
