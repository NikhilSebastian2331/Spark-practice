package whenotherwise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxRate {

  def main(args: Array[String]): Unit={

    val spark = SparkSession.builder()
      .appName("TaxRate")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val income_data = Seq((1, 40000), (2, 75000), (3, 120000)).toDF("person_id", "income")

    val res = income_data
      .withColumn("tax", when(col("income") < 50000, col("income") * 0.05)
        .when(col("income") > 50000 && col("income") <= 100000,col("income") * 0.1)
        .otherwise(col("income") * 0.15))

    res.show()

    spark.close()
  }

}
