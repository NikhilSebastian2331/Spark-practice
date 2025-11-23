import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object salary {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("salary")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, "Alice", "Engineering", 90000),
      (2, "Bob", "Engineering", 95000),
      (3, "Charlie", "Engineering", 95000),
      (4, "David", "Engineering", 88000),
      (5, "Eve", "HR", 70000),
      (6, "Frank", "Engineering", 91000)
    ).toDF("id", "name", "department", "salary")

    val window = Window.partitionBy(col("department")).orderBy(desc("salary"))

    val rank = data.withColumn("result", dense_rank().over(window))

    val highest_sal = data.groupBy(col("department"))
      .agg(max(col("salary")).alias("highest_salary"))

    val res = rank.join(highest_sal, "department")
      .filter(col("salary") < col("highest_salary"))
      .orderBy(desc("salary"))
      .limit(1)

    val final_res = highest_sal.join(res, "department", "left")
      .select(col("department"), col("salary"))

    final_res.show()

    spark.stop()
  }

}
