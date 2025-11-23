package join

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Date

object avgSellPrice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("avgSellPrice")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pricesDF = Seq(
      // (product_id, start_date, end_date, price)
      (1, Date.valueOf("2019-02-17"), Date.valueOf("2019-02-28"), 5),
      (1, Date.valueOf("2019-03-01"), Date.valueOf("2019-03-22"), 20),
      (2, Date.valueOf("2019-01-20"), Date.valueOf("2019-02-09"), 10),
      (2, Date.valueOf("2019-02-10"), Date.valueOf("2019-03-30"), 15)
    ).toDF("product_id", "start_date", "end_date", "price")


    val unitsSoldDF = Seq(
      // (product_id, purchase_date, units)
      (1, Date.valueOf("2019-02-25"), 100),
      (1, Date.valueOf("2019-03-01"), 15),
      (2, Date.valueOf("2019-02-10"), 200),
      (2, Date.valueOf("2019-03-22"), 30)
    ).toDF("product_id", "purchase_date", "units")


    val df = pricesDF
        .join(unitsSoldDF, (pricesDF("product_id") === unitsSoldDF("product_id")) &&
          (col("purchase_date").between (col("start_date") , col("end_date"))), "left")
      .na.fill(Map("price" -> 0, "units" -> 1))
      .groupBy(pricesDF("product_id"))
      .agg(round(avg(col("price") * col("units")), 2).alias("avgPrice"))
      .select(col("product_id"), col("avgPrice"))
      .show()

    spark.stop()

  }

}
