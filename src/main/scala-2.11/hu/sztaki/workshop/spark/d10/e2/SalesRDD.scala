package hu.sztaki.workshop.spark.d10.e2

import hu.sztaki.workshop.spark.d10.e4.DiscountRDD
import org.apache.spark.rdd.RDD

class SalesRDDFunctions(rdd: RDD[SalesRecord]) {
  def totalSales = ???
  def discount(discountPercentage:Double) = ???
}

/**
  * Class that represents a sales record.
  */
class SalesRecord(val transactionId: String,
                  val customerId: String,
                  val itemId: String,
                  val itemValue: Double)
  extends Comparable[SalesRecord]
  with Serializable {
  /**
    * Compares two sales record.
    */
  override def compareTo(o: SalesRecord): Int = transactionId.compareTo(o.transactionId)
}

object SalesRDDFunctions {
  /**
    * @todo[7] Create our own implicit function that takes an RDD of SalesRecord
    *          and creates our own SalesRDDFunctions type.
    */
  implicit def addCustomFunctions(rdd: RDD[SalesRecord]): SalesRDDFunctions = ???
}