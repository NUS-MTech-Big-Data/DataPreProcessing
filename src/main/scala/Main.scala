import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark = SparkSession.builder.appName(getClass.getSimpleName).master("local[2]").getOrCreate()

  val extractedText = "I am&&& a new to data science!!"
  val extractedText1 = "I went to NUS today. It's a bit boring"
  val textDataFrame = spark.createDataFrame(Seq(
    (0, extractedText),(1, extractedText1))).toDF("id", "text")
  textDataFrame.show(false)

  val resultTokens = DataPreprocessing.tokenization(textDataFrame)
  resultTokens.show(false)

  val result = DataPreprocessing.removeStopWords(resultTokens)
  result.show(false)
  spark.stop()
}
