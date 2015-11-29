package hu.sztaki.workshop.spark.d10.e2

import hu.sztaki.workshop.spark.d10.e2.SalesRDDFunctions.addCustomFunctions
import org.apache.spark.SparkContext

object Sales {
  def main(args: Array[String]) {
    /**
      * @todo[8] Create Spark context, configuration, read from text file.
      */
    val sc = new SparkContext(args(0), "Sales extended")
    val dataRDD = sc.textFile(args(1))

    /**
      * @todo[9] Transform lines to SalesRecord.
      */
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),
        colValues(2),colValues(3).toDouble)
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
    println(discountRDD.collect().toList)
  }
}
