import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark = SparkSession.builder.appName(getClass.getSimpleName).master("local[2]").getOrCreate()
  val extractedText = "I am&&& a the per new to data science!! gonna4"
  val resultTokens = DataPreprocessing.tokenization(extractedText)
  resultTokens.select("words").show(false)
  val result = DataPreprocessing.removeStopWords(resultTokens)
  spark.stop()
}
