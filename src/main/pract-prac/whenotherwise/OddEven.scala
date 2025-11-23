package whenotherwise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OddEven {

  def main (args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("OddEven")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(1, 2, 3, 4, 5).toDF("num")

    val res = df
      .withColumn("Even_Odd", when(col("num") % 2 === 0, "Even")
                                        .otherwise("Odd"))

    res.show()

    spark.close()


  }

}
