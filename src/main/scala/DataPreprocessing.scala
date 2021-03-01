import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame

object DataPreprocessing {
  /**
   * Separate a sentence into array of words
   * any character other than words will be ignored
   */
  def tokenization(textDataFrame: DataFrame): DataFrame = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("\\W")
    regexTokenizer.transform(textDataFrame)
  }

  /**
   *
   * @param tokens Tokenized dataframe
   * @return Dataframe after removing all the stop words such as i , am , a the
   */
  def removeStopWords ( tokens : DataFrame): DataFrame = {
    val stopWordRemover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
    stopWordRemover.transform(tokens)
  }
}
