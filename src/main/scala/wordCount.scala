import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object wordCount {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("wordCount")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("F:\\Big data\\Spark_practice\\src\\main\\scala\\resources\\test.txt")

    //rdd.collect().foreach(f => println(f))

    val rdd2 = rdd.flatMap(f => f.split(" "))
    //rdd2.collect().foreach(f => println(f))

    val rdd3 = rdd2.map(f => (f.toLowerCase, 1))

    val rdd4 = rdd3.reduceByKey(_+_)

    val rdd5 = rdd4.filter{case (_, count) => count > 2}

    rdd5.collect().foreach(f => println(f._1))

    spark.close()

  }

}
