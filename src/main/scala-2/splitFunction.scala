import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object splitFunction {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("splitFunction")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val dept = Seq(
      ("50000.0#0#0#", "#"),
      ("0@1000.0@", "@"),
      ("1$", "$"),
      ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

    val sol = dept
      .withColumn("split_values",
        expr("split (VALUES,Delimiter)"))

    sol.show(truncate = false)

    val sol2 = sol.withColumn("extra", array_remove(col("split_values"), ""))

    sol2.show()

    spark.close()


  }

}
