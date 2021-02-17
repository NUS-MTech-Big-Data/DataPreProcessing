import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.DataFrame

object DataPreprocessing {
  /**
   * Seperate a sentence into array of words
   * any character other than words will be ignored
   */
  def tokenization(extractedText: String): DataFrame = {

    val existingSparkSession = SparkSession.builder().getOrCreate()
    val textDataFrame = existingSparkSession.createDataFrame(Seq(
      (0, extractedText))).toDF("id", "sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W")
    val regexTokenized = regexTokenizer.transform(textDataFrame)
    regexTokenized.select("sentence", "words").show(false)
    return regexTokenized;
  }
}
