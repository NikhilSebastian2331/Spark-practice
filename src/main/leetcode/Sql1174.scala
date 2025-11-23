import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.sql.Date
import java.time.LocalDate

object Sql1174 {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sql1174")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    case class Delivery(
                         delivery_id: Int,
                         customer_id: Int,
                         order_date: Date,
                         customer_pref_delivery_date: Date
                       )

    // Helper function to convert String to java.sql.Date
    def toDate(dateString: String): Date = Date.valueOf(LocalDate.parse(dateString))

    val deliveryData = Seq(
      Delivery(1, 1, toDate("2019-08-01"), toDate("2019-08-02")),
      Delivery(2, 2, toDate("2019-08-02"), toDate("2019-08-02")),
      Delivery(3, 1, toDate("2019-08-11"), toDate("2019-08-12")),
      Delivery(4, 3, toDate("2019-08-24"), toDate("2019-08-24")),
      Delivery(5, 3, toDate("2019-08-21"), toDate("2019-08-22")),
      Delivery(6, 2, toDate("2019-08-11"), toDate("2019-08-13")),
      Delivery(7, 4, toDate("2019-08-09"), toDate("2019-08-09"))
    )

    val deliveryDF = deliveryData.toDF()


    val window = Window.partitionBy(col("customer_id")).orderBy(col("order_date").asc)

    val cust_first_order = deliveryDF
      .withColumn("is_immediate", when($"order_date" === $"customer_pref_delivery_date", 1).otherwise(0))
      .withColumn("rn", row_number().over(window))

    val res = cust_first_order
      .withColumn("immediate_percentage",
        round(sum(col("is_immediate")) / count(col("customer_id")), 2))
      .show()

    spark.stop()

  }

}
