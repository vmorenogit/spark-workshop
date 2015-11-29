package hu.sztaki.workshop.spark.d10.e3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

class AlphabetRDD(prev:RDD[String]) extends RDD[String](prev){
  /**
    * @todo[14] Decide if a string is an alphabet.
    */
  def checkAlpha(str:String): Boolean = {
    if (str.length <= 0) return false

    val x =  str.charAt(0)
    x.toUpper.toInt >=65 &&  x.toUpper.toInt<=90
  }

  /**
    * @todo[15] Override the compute method of the RDD.
    *           Split the string and filter out the non-alphabets.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[String] =  {
    firstParent[String].iterator(split, context)
      .flatMap(line => line.split(" "))
      .filter(checkAlpha)
  }

  override protected def getPartitions: Array[Partition] = firstParent[String].partitions
  override val partitioner = firstParent[String].partitioner

}


object AlphabetRDD {
  def main(args: Array[String]): Unit = {

    /**
      * @todo[12] Setup again! Also, read from text file.
      */
    val conf: SparkConf = new SparkConf()
      .setAppName("Alphabet RDD")
      .setMaster(args(0))

    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.textFile(args(1))

    /**
      * @todo[13] Count the records (line of the text file).
      */
    println("total number of records in rdd :"+rdd.count)


    /**
      * @todo[16] Create the AlphabetRDD from the general RDD
      *           and count the elements again.
      */
    val alphabetRDD = new AlphabetRDD(rdd)
    println("total number of records in custom rdd :"+alphabetRDD.count)

  }
}