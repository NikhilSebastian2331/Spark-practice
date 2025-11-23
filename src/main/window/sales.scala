import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object sales {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("sales")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("A", "2025-05-01", 100),
      ("A", "2025-05-02", 200),
      ("A", "2025-05-03", 150),
      ("B", "2025-05-01", 300),
      ("B", "2025-05-02", 250),
      ("B", "2025-05-03", 400)
    ).toDF("category", "sale_date", "sales_amount")

    val window = Window.partitionBy(col("category")).orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val res = salesData.withColumn("sales_total", sum(col("sales_amount")).over(window))
      .withColumn("number", row_number().over(window))
      .show()

    spark.stop()
  }

}
