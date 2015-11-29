package hu.sztaki.workshop.spark.d04.e3

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AnomalyDetection {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("KMeans implementation")
      .setMaster("local[4]"))

    // 1.
    // Explore data. Take some lines from the CSV file.
    val rawData = sc.textFile(args(0))
    rawData.take(10).foreach(println)

    // 2.
    // Connections are labeled. How many are there of labels (e.g. there are 972781 'normal')?
    rawData.map(_.split(',').last).countByValue().toSeq.
      sortBy(_._2).reverse.foreach(println)

    // 3.
    // Some features are nonnumeric (categorical) remove them from the parsed data.
    // Parse the other features as numeric data (Vector[Double]).
    val labelsAndData = rawData.map(line => {
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    })

    val data = labelsAndData.sample(withReplacement = false, 0.1).values.cache()
    data.take(10).foreach(println)

    // Build a KMeans model using MLlib. Use the default parameters for now.
    // Take a look at the centroids.
    //
    // Notes:
    // Use org.apache.spark.mllib.clustering.KMeans.
    // (It takes a while to build the model.)
    val kmeans = new KMeans()
    val model = kmeans.run(data)

    model.clusterCenters.foreach(println)

    // How many clusters are there? See how it fits to the labels.
    val clusterLabelCount = labelsAndData.map { case (label,datum) =>
      val cluster = model.predict(datum)
      (cluster,label)
    }.countByValue
    clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster,label),count) =>
        println(f"$cluster%1s$label%18s$count%8s")
    }

    // Define two Vector's Euclidean distance
    // distToCentroid: distance of a Vector to the nearest centroid based on a KMeansModel
    // clusteringScore: average distance to the nearest centroid based on a model built with k
    println(clusteringScore(data, 2))

    // Print the cost of the model for k = 5, 10, 15 ... 40
    (5 to 40 by 5).map(k => (k, clusteringScore(data, k))).
      foreach(println)

    // Improve KMeans model building by running it multiple times and more precisely.
    // Run with k = 30, 40 ... 100
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    (30 to 100 by 10).par.map(k => (k, clusteringScore(data, k))).
      toList.foreach(println)

    // Standardize the data and run again with k = 60, 70 .. 120
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a,b) => a.zip(b).map(t => t._1 + t._2))

    val sumSquares = dataAsArray.fold(
      new Array[Double](numCols)
      )(
      (a,b) => a.zip(b).map(t => t._1 + t._2 * t._2)
      )
    val stdevs = sumSquares.zip(sums).map {
      case(sumSq,sum) => math.sqrt(n*sumSq - sum*sum)/n
    }

    val means = sums.map(_ / n)
    def normalize(datum: Vector) = {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0) (value - mean) else (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }

    val normalizedData = data.map(normalize).cache()
    (60 to 120 by 10).par.map(k =>
      (k, clusteringScore(normalizedData, k))).toList.foreach(println)

    // Use an ideal k to build the model on the whole data set and find anomalies in the
    // original data.
    // First define a threshold distance to centroid as the 100th furthest point from its centroid.
    val distances = normalizedData.map(
      datum => distToCentroid(datum, model)
    )
    val threshold = distances.top(100).last

    val lineParser = (line: String) => {
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)

      Vectors.dense(buffer.map(_.toDouble).toArray)
    }

    val originalAndData = rawData.map(line => (line, lineParser(line)))
    val normalizer = buildNormalizationFunction(data)
    val anomalies = originalAndData.filter { case (original, datum) =>
      val normalized = normalizer(datum)
      distToCentroid(normalized, model) > threshold
    }.keys
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).
      map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  def clusteringScore(data: RDD[Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
        (a, b) => a.zip(b).map(t => t._1 + t._2)
      )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }

}
