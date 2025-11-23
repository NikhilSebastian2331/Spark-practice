package interview

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object readLog {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("readLog")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.textFile("F:\\Big data\\Spark_practice\\src\\main\\pract-prac\\interview\\sample1.log")

    val res = df.select(
      split(col("value"), "\\|").getItem(0).as("timestamp"),
      split(col("value"), "\\|").getItem(1).as("level"),
      split(col("value"), "\\|").getItem(2).as("message")
    )

    res.show(truncate = false)

    spark.stop()
  }

}
