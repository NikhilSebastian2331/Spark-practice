package interview

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object lag {

  def main (args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("lag")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("2023-01-01", "Phone", 800),
      ("2023-01-02", "Phone", 900),
      ("2023-01-03", "Phone", 850),
      ("2023-01-04", "Phone", 950),
      ("2023-01-05", "Phone", 920)
    )

    val df = salesData.toDF("sale_date", "product", "revenue")

    val window = Window.partitionBy(col("product")).orderBy(col("sale_date"))

    val res = df.withColumn("previous_day_revenue",
      functions.lag("revenue", 1).over(window))
      .withColumn("revenue_change",
        col("revenue") - col("previous_day_revenue"))
      .show()

    spark.stop()
  }

}
