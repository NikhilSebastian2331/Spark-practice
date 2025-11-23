import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sql197 {

  def main (args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Sql197")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val weatherData = Seq(
      (1, "2025-03-22", 25),
      (2, "2025-03-23", 28),
      (3, "2025-03-24", 27),
      (4, "2025-03-25", 30)
    ).toDF("id", "recordDate", "temperature")

    val weatherDF = weatherData.withColumn("recordDate", to_date(col("recordDate"),
      "yyyy-MM-dd"))


    val res = weatherDF.as("W1")
      .join(weatherDF.as("W2"),
        col("W1.recordDate") === date_add(col("W2.recordDate"), 1))
      .filter(col("W1.temperature") > col("W2.temperature"))
      .select(col("W1.id"))
      .show()

    spark.stop()
  }

}
