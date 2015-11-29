package hu.sztaki.workshop.spark.d03.e3

import hu.sztaki.workshop.spark.d03.e3.AdvancedRDD.RichRDD
import org.apache.spark.{SparkConf, SparkContext}

object BigramAnalysis{
  def main(args: Array[String]){
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("Bigram analysis")
        .setMaster("local")
    )

    val linesRDD = sc.textFile(args(0))

    /**
      * @todo[1] Transform to `Bigram`s and filter invalid items.
      * @hint Use the companion object of `model.Bigram`.
      */
    val bigramsRDD = linesRDD
      .flatMap(Bigram(_))
      .filter(_.isValidBigram)

    /**
      * @todo[2] Total number of bigrams
      */
    println(
      bigramsRDD.count()
    )

    /**
      * @todo[3] Cache bigrams.
      */
    bigramsRDD.cache()

    /**
      * @todo[4] How many unique bigrams do we have?
      */
    val uniqeBigrams = bigramsRDD.distinct().count()
    print(uniqeBigrams)

    /**
      * @todo[5] Count each element.
      * @hint Use the AdvancedRDD (implicitly).
      */
    val bgOccrCount =
      bigramsRDD.countEachElement

    /**
      * @todo[6] Number of bigrams that appear only once.
      */
    println(
      bgOccrCount.countWhere(bgCountPair => bgCountPair._2 == 1)
    )

    /**
      * @todo[7] List the top ten most frequent bigrams and their counts.
      */
    val bgOccrCountSorted = bgOccrCount.sortByDesc(_._2)
    bgOccrCountSorted.take(10).foreach(println(_))
    bgOccrCountSorted.take(10).foreach(println)
    bgOccrCountSorted.take(10) foreach { println }
    bgOccrCountSorted.take(10) foreach println
    bgOccrCountSorted take 10 foreach println

    /**
      * @todo[8] What fraction of all bigrams occurrences does the top ten bigrams account for?
      *          That is, what is the cumulative frequency of the top ten bigrams?
      */
    val totalBgCount = bgOccrCount.values.sum

    val topTenBgOccrCount = bgOccrCountSorted
      .take(10).map(_._2).sum

    val fractionTopTenBigramOccurance = topTenBgOccrCount / totalBgCount

    println(
      fractionTopTenBigramOccurance
    )


    /**
      * @todo[9*] Determine the frequency of bigrams with the same start.
      * @hint Use `BigramsWithSameStart` and aggregateByKey also.
      */
    val startingWordBigram =  bigramsRDD.map(bg => (bg.firstWord, bg))

    val startingWordAllBigrams = startingWordBigram
      .aggregateByKey(BigramsWithSameStart("", List()))(
        (acc, bigram) => BigramsWithSameStart.apply(bigram),
        (bgs1, bgs2) => bgs1.merge(bgs2))

    val startWordBGCount = startingWordAllBigrams.map(a => (a._1, a._2.bigramsCount))

    val startWordBGAndBGCount = bgOccrCount.map(bg => (bg._1.firstWord, (bg._1, bg._2)))

    //  [(String, ((String, Int)), Int)]
    //  startWord - BG           - BG count    - bgs starting with word
    //  a._1      - a._2._1._1   - a._2._1._2  - a._2._2
    val startWordBGbGCountStartWordBgsCount = startWordBGAndBGCount.join(startWordBGCount).map(a => (a._1, a._2._1._1, a._2._1._2, a._2._2))

    startWordBGbGCountStartWordBgsCount.cache()
    startWordBGbGCountStartWordBgsCount foreach println

    /**
      * @todo[10] What are the five most frequent words following the word "for"?
      * @todo[11] What is the frequency of observing each word?
      */
    startWordBGbGCountStartWordBgsCount
      .filter(a => a._1.equals("for"))
      .map(a => (a._1, a._2, a._4/a._3.toFloat))
      .sortBy(a => a._3)
      .take(5) foreach println

    startWordBGbGCountStartWordBgsCount
      .filter(a => a._1.equals("late"))
      .map(a => (a._1, a._2, a._4/a._3.toFloat))
      .sortBy(a => a._3)
      .take(5) foreach println

    /**
      * If there are a total of N words in your vocabulary,
      * then there are a total of N^2 possible values for F(Wn|Wn-1)—in theory,
      * every word can follow every other word (including itself).
      * @todo[12] What fraction of these values are non-zero?
      * In other words, what proportion of all possible events are actually observed?
      * To give a concrete example, let's say that following the word "happy",
      * you only observe 100 different words in the text collection.
      * This means that N-100 words are never seen after "happy".
      * (Perhaps the distribution of happiness is quite limited?).
      */
    val allWords = linesRDD.flatMap(line => line.split(" "))
    val allDistinctWordsCount = allWords.distinct().count()
    val totalPossibleCombinations = allDistinctWordsCount * allDistinctWordsCount

    val allDistinctBigrams = bigramsRDD.distinct().count()

    val fractionOfBgsFoundOutOFTotalPossible = allDistinctBigrams / totalPossibleCombinations.toFloat
    println(fractionOfBgsFoundOutOFTotalPossible)
  }
}



