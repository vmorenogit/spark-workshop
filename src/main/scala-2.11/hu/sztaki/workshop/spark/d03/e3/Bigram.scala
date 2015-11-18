package hu.sztaki.workshop.spark.d03.e3

case class Bigram(firstWord: String, secondWord: String) {
  def isValidBigram: Boolean = !firstWord.equals(" ") && !secondWord.equals(" ")
}

case class BigramsWithSameStart(firstWord: String, bigrams: List[String]) {
  def bigramsCount = bigrams.size

  def merge(bg2: BigramsWithSameStart): BigramsWithSameStart = {
    new BigramsWithSameStart(firstWord, bigrams ++ bg2.bigrams)
  }
}

object BigramsWithSameStart {
  def apply(bigram: Bigram) = {
    new BigramsWithSameStart(bigram.firstWord, List(bigram.secondWord))
  }
}

object Bigram {
  /**
    * @todo Complete function.
    */
  def apply(input: String): List[Bigram] = {
    val splitWords = input.split(" ")
    if(splitWords.size < 2) return List[Bigram]()
    val slidingWords = splitWords.sliding(2).toList
    slidingWords.map(words => new Bigram(words(0), words(1)))
  }
}
