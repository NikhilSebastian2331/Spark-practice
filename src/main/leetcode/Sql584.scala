import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.language.postfixOps

object Sql584 {

  def main(args: Array[String]):Unit ={

    val spark = SparkSession.builder()
      .appName("Sql584")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("referee_id", IntegerType, true)
    ))

    val data = Seq(
      (1, "Will", null),
      (2, "Jane", null),
      (3, "Alex", 2),
      (4, "Bill", null),
      (5, "Zack", 1),
      (6, "Mark", 2)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data.map(Row.fromTuple)),
      schema
    )

    val res = df
        .filter(col("referee_id") =!= 2 || col("referee_id").isNull)

    res.select(col("name")).show()

    spark.stop()

  }
}
