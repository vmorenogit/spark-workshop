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
    val tf = new HashingTF(100)

    // Each email is split into words, and each word is mapped to one feature.
    val hamFeatures =
      ham.map(email => tf.transform(email.split(' ')))
    val spamFeatures =
      spam.map(email => tf.transform(email.split(' ')))

    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.

    // spam
    val labSpam = spamFeatures.map(LabeledPoint(1, _))
    // ham
    val labHam = hamFeatures.map(LabeledPoint(0, _))

    val trainingData = labSpam ++ labHam

    // Cache data since Logistic Regression is an iterative algorithm.

    // Create a Logistic Regression learner.
    // Run the actual learning algorithm on
    // the training data.
    val logReg = new LogisticRegressionWithSGD()
    val model = logReg.run(trainingData)

    // Test on a positive example (spam) and a negative one (ham).
    // First apply the same HashingTF feature transformation used on the training data.
    val posExample =
      "O M G GET cheap stuff by sending money to ..."
    val negExample =
      "Hi Dad, I started studying Spark the other ..."

    println("pos example: " +
      model.predict(
        tf.transform(posExample.split(' '))))

    println("neg example: " +
      model.predict(
        tf.transform(negExample.split(' '))))

    // Now use the learned model to predict spam/ham for new emails.
  }
}
