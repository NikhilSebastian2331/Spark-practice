package interview

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object logFile {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("logFile")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.textFile("F:\\Big data\\Spark_practice\\src\\main\\pract-prac\\interview\\sample.log")
      .filter(lower(col("value")).contains("error")
        || lower(col("value")).contains("warn"))

    df.show(truncate = false)

    spark.stop()
  }

}
