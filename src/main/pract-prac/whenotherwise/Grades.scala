package whenotherwise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Grades {

  def main (args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Grades")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val students = Seq((1, 95), (2, 85), (3, 72), (4, 45)).toDF("student_id", "marks")

    val res = students
      .withColumn("grade", when(col("marks") >= 90, "A")
      .when(col("marks") < 90 && col("marks") >= 70, "B")
      .when(col("marks") < 70 && col("marks") >= 50, "C")
      .otherwise("F"))

    res.show()

    spark.close()
  }
}
