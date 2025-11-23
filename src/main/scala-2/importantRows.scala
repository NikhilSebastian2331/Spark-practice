import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object importantRows {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("importantRows")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val input = Seq(
      (1, "MV1"),
      (1, "MV2"),
      (2, "VPV"),
      (2, "Others")).toDF("id", "value")

    val windowSpec = Window.partitionBy("id").orderBy($"value")
    val ranked = input.withColumn("rank", row_number.over(windowSpec))

    val sol = ranked.filter($"rank" === 1).drop("rank").withColumnRenamed("value", "name")

    sol.show()

    spark.close()
  }

}
