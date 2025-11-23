import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object rank {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("rank")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    val data = Seq(
      (101, "T001", 500.75, "2025-05-01"),
      (102, "T002", 1200.00, "2025-05-02"),
      (103, "T003", 350.40, "2025-05-03"),
      (101, "T004", 220.00, "2025-05-04"),
      (104, "T005", 980.50, "2025-05-05"),
      (102, "T006", 450.90, "2025-05-06"),
      (105, "T007", 750.25, "2025-05-07"),
      (103, "T008", 1100.60, "2025-05-08"),
      (104, "T009", 400.30, "2025-05-09"),
      (105, "T010", 1800.00, "2025-05-10")
    ).toDF("customer_id", "transaction_id", "amount", "date")

    //val window = Window.partitionBy(col("customer_id")).orderBy(desc("amount"))

    //val res = data.withColumn("rank", row_number().over(window))
      //.filter(col(""))

   val totalSpent = data.groupBy(col("customer_id"))
     .agg(sum(col("amount")).alias("total_spent"))

   val window = Window.orderBy(desc("total_spent"))

   val res = totalSpent.withColumn("rank", functions.rank().over(window))
     .filter(col("rank") <= 10)
     .show()

    spark.stop()

  }

}
