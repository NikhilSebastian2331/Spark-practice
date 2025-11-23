package join

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object pojEmpl {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("projEmpl")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val projectDF = Seq(
      // (project_id, employee_id)
      (1, 1),
      (1, 2),
      (2, 1),
      (3, 3),
      (3, 4),
      (3, 1)
    ).toDF("project_id", "employee_id")

    val employeeDF = Seq(
      // (employee_id, name, experience_years)
      (1, "Alice", 3),
      (2, "Bob", 5),
      (3, "Charlie", 1),
      (4, "David", 8)
    ).toDF("employee_id", "name", "experience_years")

    val res = projectDF
      .join(employeeDF, projectDF("employee_id") === employeeDF("employee_id"), "inner")
      .groupBy(projectDF("project_id"))
      .agg(round(avg(col("experience_years")), 2).as("average_years"))
      .select(projectDF("project_id"), col("average_years"))
      .show()

    spark.stop()


  }

}
