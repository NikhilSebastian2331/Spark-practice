import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object limitSet4 {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("limitSet4")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val input = spark.range(50).withColumn("key", $"id" % 5)

    val st1 = input.groupBy("key")
      .agg(array_sort(collect_set("id")).alias("all"),
      slice(array_sort(collect_set("id")), 1, 3).alias("only_first_three"))

    st1.show()

    spark.close()
  }
}
