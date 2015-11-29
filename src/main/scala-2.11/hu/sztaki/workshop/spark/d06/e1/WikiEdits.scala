package hu.sztaki.workshop.spark.d6.e1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.RegexTokenizer

object WikiEdits {

  def main(args: Array[String]) {

    //    display(dbutils.fs.ls("dbfs:/mnt/wikipedia-readonly-eu/eng_wikipedia_parquet_articles_only/"))

    val sc = new SparkContext(new SparkConf().setAppName("WikiEdits"))
    val sqlContext = new SQLContext(sc)

    val wikiDF = sqlContext.read.parquet("dbfs:/mnt/wikipedia-readonly-eu/eng_wikipedia_parquet_articles_only/").cache()

    wikiDF.printSchema()

    wikiDF.show(2)

    wikiDF.count()

    wikiDF.count()

    wikiDF.registerTempTable("wikipedia")

    sqlContext.sql("SELECT COUNT(*) FROM wikipedia WHERE revisionTimestamp >= DATE '2015-09-05'")

    sqlContext.sql("SELECT * FROM wikipedia WHERE revisionTimestamp >= DATE '2015-09-05' LIMIT 12")

    sqlContext.sql("SELECT COUNT(*) FROM wikipedia WHERE revisionUsername = \"ClueBot NG\"")

    sqlContext.sql("SELECT * FROM wikipedia WHERE revisionUsername = \"ClueBot NG\" LIMIT 20")

    sqlContext.sql("SELECT revisionUsername, COUNT(revisionUsername) FROM wikipedia GROUP BY revisionUsername ORDER BY COUNT(revisionUsername) DESC; ")

    // Register a function that can search that a string is found.
    // You could just use the sql "like" syntax, but this demonstrates how to register a function which you can do more with.
    val containsWord = (s: String, w: String) => {
      (s != null && s.indexOfSlice(w) >= 0).toString()
    }
    sqlContext.udf.register("containsWord", containsWord)

    sqlContext.sql("select title from wikipedia where containsWord(text, '$word') == 'true'")

    val cleanAndSplit = (somestring: String) => {
      if (somestring == null) {
        Array[String]()
      } else {
        somestring.toLowerCase().replaceAll("[^\\w]", " ").trim().split("\\s").filter(s => s.length() > 0)
      }
    }

    case class WordCount(word: String, count: Long)

    val wordCountsRdd = (wikiDF.rdd
      .flatMap(row => cleanAndSplit(row.getString(0)))
      .map(s => (s, 1))
      .reduceByKey(_ + _)
      .map(t => WordCount(t._1, t._2)))

    sqlContext.sql("select word, count from word_counts order by count desc limit 10")

    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("\\W+")
    val wikiWordsDF = tokenizer.transform(wikiDF)

//    wikiWordsDF.select($"title", $"words").first

    wikiWordsDF.show(5)

  }
}