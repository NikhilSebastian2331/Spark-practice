package join

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object contest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("contest")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val usersDF = Seq(
      // (user_id, user_name)
      (6, "Alice"),
      (2, "Bob"),
      (7, "Alex")
    ).toDF("user_id", "user_name")

    val registerDF = Seq(
      // (contest_id, user_id)
      (215, 6),
      (209, 2),
      (208, 2),
      (210, 6),
      (208, 6),
      (209, 7),
      (209, 6),
      (215, 7),
      (208, 7),
      (210, 2),
      (207, 2),
      (210, 7)
    ).toDF("contest_id", "user_id")

    //val subDF = usersDF
      //.select(count(col("user_id")).as("tot_users"))

    val res = registerDF
      .withColumn("tot_users", count(usersDF("user_id")))
      .groupBy(col("contest_id"))
      .agg(count(col("contest_id")) * 100 / col("tot_users").as("percentage"))
      .select(col("user_id"), col("percentage"))
      .show()

    spark.stop()
  }

}
