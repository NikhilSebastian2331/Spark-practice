package interview
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object window1 {

  def main (args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("window1")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("HR", "Alice", 60000),
      ("HR", "Bob", 60000),
      ("HR", "Charlie", 58000),
      ("IT", "David", 72000),
      ("IT", "Eve", 72000),
      ("IT", "Frank", 69000),
      ("IT", "Grace", 69000)
    )

    val df = data.toDF("department", "employee", "salary")

    val window = Window.partitionBy(col("department")).orderBy(col("salary").desc)

    val res = df.withColumn("row_number", row_number().over(window))
      .withColumn("rank", rank().over(window))
      .withColumn("dens_rank", dense_rank().over(window))
      .show()

    spark.stop()
  }

}
