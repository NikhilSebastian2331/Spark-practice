import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object splitFunction1 {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("splitFunction1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val dept = Seq(
      ("50000.0#0#0#", "#"),
      ("0@1000.0@", "@"),
      ("1$", "$"),
      ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

    dept.show(truncate = false)

    val solution = dept.withColumn("split_values",
      expr("split(VALUES, Delimiter)"))

    solution.show(truncate = false)

    val extra = solution.withColumn("extra",
      expr("filter(split_values, x -> x != '')"))

    extra.show(truncate = false)

    spark.stop()

  }

}
