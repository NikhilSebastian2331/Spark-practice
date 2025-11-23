import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sql1581 {

  def main (args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Sql1581")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val visitsDF = Seq(
      (1, 23),
      (2, 9),
      (4, 30),
      (5, 54),
      (6, 96),
      (7, 54),
      (8, 54)
    ).toDF("visit_id", "customer_id")

    // Creating Transactions DataFrame using Seq and toDF
    val transactionsDF = Seq(
      (2, 5, 310),
      (3, 5, 300),
      (9, 5, 200),
      (12, 1, 910),
      (13, 2, 970)
    ).toDF("transaction_id", "visit_id", "amount")

    val res = visitsDF
      .join(transactionsDF,
        visitsDF.col("visit_id") === transactionsDF.col("visit_id"),
        "left_outer")
      .filter(col("transaction_id").isNull)
      .groupBy(visitsDF.col("customer_id"))

    res.agg(count(visitsDF.col("visit_id")) alias("count_no_trans"))
      .select(visitsDF.col("customer_id"), col("count_no_trans"))
      .show()

    spark.stop()



  }

}
