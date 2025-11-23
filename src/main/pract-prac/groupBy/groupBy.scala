package groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object groupBy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("groupBy")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1,"manish",50000,"IT"),
      (2,"vikash",60000,"sales"),
      (3,"raushan",70000,"marketing"),
      (4,"mukesh",80000,"IT"),
      (5,"pritam",90000,"sales"),
      (6,"nikita",45000,"marketing"),
      (7,"ragini",55000,"marketing"),
      (8,"rakesh",100000,"IT"),
      (9,"aditya",65000,"IT"),
      (10,"rahul",50000,"marketing")
    ).toDF("id", "name", "salary", "dept")

    df.select(col("dept"), col("salary"))
      .groupBy(col("dept"))
      .agg(sum(col("salary")))
      .show()

    spark.stop()
  }
}
