package hu.sztaki.workshop.spark.d07.e2

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

object SpamFiltering {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Spam filtering")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load 2 types of emails from text files: spam and ham (non-spam).
    // Each line has text from one email.
    val spam = sc.textFile(args(0))
    val ham = sc.textFile(args(1))

    spam.collect().foreach(println)
    ham.collect().foreach(println)

    // Create a HashingTF instance to map email text to vectors of 100 features.

    // Each email is split into words, and each word is mapped to one feature.

    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.

    // Cache data since Logistic Regression is an iterative algorithm.

    // Create a Logistic Regression learner.

    // Run the actual learning algorithm on the training data.

    // Test on a positive example (spam) and a negative one (ham).
    // First apply the same HashingTF feature transformation used on the training data.
    val posExample = "O M G GET cheap stuff by sending money to ..."
    val negExample = "Hi Dad, I started studying Spark the other ..."

    // Now use the learned model to predict spam/ham for new emails.
  }
}
