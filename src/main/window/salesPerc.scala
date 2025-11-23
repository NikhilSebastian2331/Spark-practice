import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object salesPerc {

  def main (args: Array[String]): Unit = {

    val spark =  SparkSession.builder()
      .appName("salesPerc")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, "iphone", "01-01-2023", 1500000),
      (2, "samsung", "01-01-2023", 1100000),
      (3, "oneplus", "01-01-2023", 1100000),
      (1, "iphone", "01-02-2023", 1300000),
      (2, "samsung", "01-02-2023", 1120000),
      (3, "oneplus", "01-02-2023", 1120000),
      (1, "iphone", "01-03-2023", 1600000),
      (2, "samsung", "01-03-2023", 1080000),
      (3, "oneplus", "01-03-2023", 1160000),
      (1, "iphone", "01-04-2023", 1700000),
      (2, "samsung", "01-04-2023", 1800000),
      (3, "oneplus", "01-04-2023", 1170000),
      (1, "iphone", "01-05-2023", 1200000),
      (2, "samsung", "01-05-2023", 980000),
      (3, "oneplus", "01-05-2023", 1175000),
      (1, "iphone", "01-06-2023", 1100000),
      (2, "samsung", "01-06-2023", 1100000),
      (3, "oneplus", "01-06-2023", 1200000)
    ).toDF("product_id", "product_name", "sale_date", "sales_amount")

    val window = Window.partitionBy(col("product_name"))

    //val sum = data.groupBy(col("product_name"))
      //.agg(functions.sum(col("sales_amount")).alias("total"))

    val res = data.withColumn("total_sale", sum(col("sales_amount")).over(window))
      .withColumn("sales_perc",
      round((col("sales_amount") / col("total_sale") * 100), 2))

    res.show()

    spark.stop()
  }

}
