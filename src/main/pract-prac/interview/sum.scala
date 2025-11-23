package interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object sum {

  def main (args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("sum")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("2023-01-01", "Laptop", 1000),
      ("2023-01-05", "Laptop", 1500),
      ("2023-01-10", "Laptop", 1200),
      ("2023-01-02", "Phone", 800),
      ("2023-01-07", "Phone", 600),
      ("2023-01-15", "Phone", 900)
    )

    val df = salesData.toDF("sale_date", "product", "revenue")

    val window = Window.partitionBy(col("product")).orderBy(col("sale_date"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val res = df.withColumn("cumulative_revenue", org.apache.spark.sql.functions.sum("revenue").over(window))
      .show()

    spark.stop()
  }



}
