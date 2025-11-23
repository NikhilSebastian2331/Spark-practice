import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object lat_tran {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lat_tran")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val transactionsDF = Seq(
      ("user1", "txn1", "2025-05-18 10:30:00"),
      ("user2", "txn2", "2025-05-18 11:00:00"),
      ("user1", "txn3", "2025-05-18 12:00:00"),
      ("user3", "txn4", "2025-05-18 09:45:00"),
      ("user2", "txn5", "2025-05-18 11:30:00")
    ).toDF("user_id", "transaction_id", "transaction_time")

    val tranDF = transactionsDF.withColumn("transaction_time",
      col("transaction_time").cast("timestamp"))

    val window = Window.partitionBy(col("user_id"))
      .orderBy(desc("transaction_time"))

    val res = tranDF.withColumn("lat_tran", row_number().over(window))
      .filter(col("lat_tran") === 1)

    res.show()

    spark.stop()
  }

}
