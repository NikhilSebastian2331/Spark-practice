package whenotherwise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object salarGrade {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("salarGrade")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val salary = Seq(
      (1, "Jhon", 4000),
      (2, "Tim David", 12000),
      (3, "Json Bhrendroff", 7000),
      (4, "Jordon", 8000),
      (5, "Green", 14000),
      (6, "Brewis", 6000)
    ).toDF("emp_id", "emp_name", "salary")

    val res = salary
      .withColumn("salary_grade",
        when(col("salary")<= 4000, "C")
          .when(col("salary").between(5000, 10000), "B")
          .otherwise("A")
      )
      .show()

    spark.stop()

  }
}


