import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sql595 {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Sql595")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("Afghanistan", "Asia", 652230, 25500100, 20343000000L),
      ("Albania", "Europe", 28748, 2831741, 12960000000L),
      ("Algeria", "Africa", 2381741, 37100000, 188681000000L),
      ("Andorra", "Europe", 468, 78115, 3712000000L),
      ("Angola", "Asia", 1246700, 20609294, 100990000000L)
    ).toDF("name", "continent", "area", "population", "gdp")

    val res = df
      .filter(col("area") >= 3000000 || col("population") >= 25000000)

    res.select(col("name"), col("population"), col("area")).show()

    spark.stop()
  }
}
