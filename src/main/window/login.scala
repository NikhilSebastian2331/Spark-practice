import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object login {

  def main (args: Array [String]): Unit = {

    val spark = SparkSession.builder()
      .appName("login")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val loginData = Seq(
      ("user1", "2025-05-01"),
      ("user1", "2025-05-02"),
      ("user1", "2025-05-03"),
      ("user1", "2025-05-05"),
      ("user2", "2025-05-02"),
      ("user2", "2025-05-03"),
      ("user2", "2025-05-04"),
      ("user3", "2025-05-01"),
      ("user3", "2025-05-05")
    ).toDF("user_id", "login_date")

    val df = loginData.withColumn("login_date",
      to_date(col("login_date"), "yyyy-MM-dd"))

    val window = Window.partitionBy(col("user_id")).orderBy(col("login_date"))

    val date_diff = df.withColumn("date_diff",
      functions.date_diff(col("login_date"),
        lag(col("login_date"), 1).over(window)))

    val streak_count = date_diff.withColumn("streak_cnt",
      sum(when(col("date_diff") === 1, lit(0)).otherwise(lit(1))).over(window))

    val streak = streak_count.groupBy("user_id", "streak_cnt")
      .agg(count("*").alias("streak_length"))



    //streak_count.show()
    //streak.show()

    spark.stop()

  }

}
