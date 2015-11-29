package hu.sztaki.workshop.spark.d6.e1

import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object WikiPageviews {

  def main(args: Array[String]) {

    val x = 1 + 7

    val y = 2 + x

    println(s"This was last run on: ${new Date}")

    val sc = new SparkContext(new SparkConf().setAppName("WikiPageViews"))
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val pageviewsDF = sqlContext.read.table("pageviews_by_second")

    pageviewsDF.show()

    // The display() function also shows the DataFrame, but in a prettier HTML format (this only works in Databricks notebooks)

//    display(pageviewsDF)

    pageviewsDF.printSchema()

    // The following orders the rows by first the timestamp (ascending), then the site (descending) and then shows the first 10 rows

    pageviewsDF.orderBy($"timestamp", $"site".desc).show(10)

    // Count how many total records (rows) there are
    pageviewsDF.count()

    sqlContext.cacheTable("pageviews_by_second")

    pageviewsDF.count()

    pageviewsDF.filter($"site" === "mobile").count()

    pageviewsDF.filter($"site" === "desktop").count()

    pageviewsDF.groupBy($"site").count().show()

    pageviewsDF.select(sum($"requests")).show()

    pageviewsDF.filter("site = 'mobile'").select(sum($"requests")).show()

    pageviewsDF.filter("site = 'desktop'").select(sum($"requests")).show()

    // Currently in our DataFrame, `pageviewsDF`, the first column is typed as a string
    pageviewsDF.printSchema()

    val pageviewsDF2 = pageviewsDF.select($"timestamp".cast("timestamp").alias("timestamp"), $"site", $"requests")

    pageviewsDF2.printSchema()

//    display(pageviewsDF2)

    pageviewsDF2.select(year($"timestamp")).distinct().show()

    pageviewsDF2.select(month($"timestamp")).distinct().show()

    pageviewsDF2.select(weekofyear($"timestamp")).distinct().show()

    pageviewsDF2.select(dayofyear($"timestamp")).distinct().count()

    pageviewsDF2.filter("site = 'mobile'").select(avg($"requests"), min($"requests"), max($"requests")).show()

    pageviewsDF2.filter("site = 'desktop'").select(avg($"requests"), min($"requests"), max($"requests")).show()

    // Notice the use of alias() to rename the new column
    // "E" is a pattern in the SimpleDataFormat class in Java that extracts out the "Day in Week""

    // Create a new DataFrame named pageviewsByDayOfWeekDF and cache it
    val pageviewsByDayOfWeekDF = pageviewsDF2.groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().cache()

    // Show what is in the new DataFrame
    pageviewsByDayOfWeekDF.show()

//    display(pageviewsByDayOfWeekDF.orderBy($"Day of week"))

    def matchDayOfWeek(day:String): String = {
      day match {
        case "Mon" => "1-Mon"
        case "Tue" => "2-Tue"
        case "Wed" => "3-Wed"
        case "Thu" => "4-Thu"
        case "Fri" => "5-Fri"
        case "Sat" => "6-Sat"
        case "Sun" => "7-Sun"
        case _ => "UNKNOWN"
      }
    }

    matchDayOfWeek("Tue")

    val prependNumberUDF = sqlContext.udf.register("prependNumber", (s: String) => matchDayOfWeek(s))

    pageviewsByDayOfWeekDF.select(prependNumberUDF($"Day of week")).show(7)

//    display((pageviewsByDayOfWeekDF.withColumnRenamed("sum(requests)", "total requests")
//      .select(prependNumberUDF($"Day of week"), $"total requests")
//      .orderBy("UDF(Day of week)")))

    val mobileViewsByDayOfWeekDF = pageviewsDF2.filter("site = 'mobile'").groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().withColumnRenamed("sum(requests)", "total requests").select(prependNumberUDF($"Day of week"), $"total requests").orderBy("UDF(Day of week)").toDF("DOW", "mobile_requests")

    // Cache this DataFrame
    mobileViewsByDayOfWeekDF.cache()

//    display(mobileViewsByDayOfWeekDF)

    val desktopViewsByDayOfWeekDF = pageviewsDF2.filter("site = 'desktop'").groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().withColumnRenamed("sum(requests)", "total requests").select(prependNumberUDF($"Day of week"), $"total requests").orderBy("UDF(Day of week)").toDF("DOW", "desktop_requests")

    // Cache this DataFrame
    desktopViewsByDayOfWeekDF.cache()

//    display(desktopViewsByDayOfWeekDF)



  }
}