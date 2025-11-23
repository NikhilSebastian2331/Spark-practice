import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sql1661 {

  def main (args: Array[String]): Unit={

    val spark = SparkSession.builder()
      .appName("Sql1661")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (0, 0, "start", 0.712),
      (0, 0, "end", 1.520),
      (0, 1, "start", 3.140),
      (0, 1, "end", 4.120),
      (1, 0, "start", 0.550),
      (1, 0, "end", 1.550),
      (1, 1, "start", 0.430),
      (1, 1, "end", 1.420),
      (2, 0, "start", 4.100),
      (2, 0, "end", 4.512),
      (2, 1, "start", 2.500),
      (2, 1, "end", 5.000)
    )
    val activityDF = data.toDF("machine_id", "process_id", "activity_type", "timestamp")

    activityDF.as("a1")
      .join(activityDF.as("a2"),
        col("a1.machine_id") === col("a2.machine_id") &&
        col("a1.process_id") === col("a2.process_id") &&
        col("a1.activity_type") === lit("start") &&
          col("a2.activity_type") === lit("end")
      ,"inner")
      .withColumn("processing_time", col("a2.timestamp") - col("a1.timestamp"))
      .groupBy(col("a1.machine_id"))
      .agg(round(avg(col("processing_time")), 3).as("processing_time"))
      .select(col("machine_id"), col("processing_time"))
      .show()

    spark.stop()
  }

}
