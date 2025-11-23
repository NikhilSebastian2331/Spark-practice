package interview

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object sale_diff {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("sale_diff")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val product_data = Seq(
      (2, "samsung", "01-01-1995", 11000),
      (1, "iphone", "01-02-2023", 1300000),
      (2, "samsung", "01-02-2023", 1120000),
      (3, "oneplus", "01-02-2023", 1120000),
      (1, "iphone", "01-03-2023", 1600000),
      (2, "samsung", "01-03-2023", 1080000),
      (3, "oneplus", "01-03-2023", 1160000),
      (1, "iphone", "01-01-2006", 15000),
      (1, "iphone", "01-04-2023", 1700000),
      (2, "samsung", "01-04-2023", 1800000),
      (3, "oneplus", "01-04-2023", 1170000),
      (1, "iphone", "01-05-2023", 1200000),
      (2, "samsung", "01-05-2023", 980000),
      (3, "oneplus", "01-05-2023", 1175000),
      (1, "iphone", "01-06-2023", 1100000),
      (3, "oneplus", "01-01-2010", 23000),
      (2, "samsung", "01-06-2023", 1100000),
      (3, "oneplus", "01-06-2023", 1200000)
    ).toDF("product_id", "product_name", "sales_date", "sales")

    val df = product_data.withColumn("sales_date", to_date(col("sales_date"), "dd-MM-yyyy"))

    val window = Window.partitionBy(col("product_id")).orderBy(col("sales_date"))

    val res = df
      .withColumn("first_sale", first(col("sales")).over(window))
      .withColumn("last_sale", last(col("sales")).over(window))
      .withColumn("sales_diff", col("last_sale") - col("first_sale"))
      .select("product_id", "product_name", "sales_diff").distinct()
      .show()

    spark.stop()


  }

}
