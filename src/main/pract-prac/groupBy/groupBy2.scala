package groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object groupBy2 {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("groupBy2")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1, "manish", 50000, "IT", "india"),
      (2, "vikash", 60000, "sales", "us"),
      (3, "raushan", 70000, "marketing", "india"),
      (4, "mukesh", 80000, "IT", "us"),
      (5, "pritam", 90000, "sales", "india"),
      (6, "nikita", 45000, "marketing", "us"),
      (7, "ragini", 55000, "marketing", "india"),
      (8, "rakesh", 100000, "IT", "us"),
      (9, "aditya", 65000, "IT", "india"),
      (10, "rahul", 50000, "marketing", "us")
    ).toDF("id", "name", "salary", "dept", "country")

    df.select(col("dept"), col("salary"), col("country"))
      .groupBy(col("dept"), col("country"))
      .agg(sum(col("salary")))
      .show()

    spark.stop()
  }

}
