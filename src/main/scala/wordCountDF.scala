import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object wordCountDF {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("wordCountDF")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df1 = spark.read.textFile("F:\\Big data\\Spark_practice\\src\\main\\scala\\resources\\test.txt")

    //df1.show()

    val wordCount = df1
      .select(explode(split(col("value"), "//s+")).alias("word"))
      .withColumn("word", lower(col("word")))
      .groupBy("word")
      .count()
      .withColumnRenamed("count", "wordCount")

    val sol = wordCount.filter(col("wordCount") > 2)
    sol.show()


    spark.close()
  }

}
