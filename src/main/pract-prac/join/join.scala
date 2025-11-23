package join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


object join {

  def main (args: Array[String]): Unit={

    val spark = SparkSession.builder()
      .appName("Create DataFrames")
      .master("local[*]")
      .getOrCreate()

    val customerSchema = StructType(Array(
      StructField("customer_id", IntegerType, nullable = true),
      StructField("customer_name", StringType, nullable = true),
      StructField("address", StringType, nullable = true),
      StructField("date_of_joining", StringType, nullable = true)
    ))

    // Customer data
    val customerData = Seq(
      (1, "manish", "patna", "30-05-2022"),
      (2, "vikash", "kolkata", "12-03-2023"),
      (3, "nikita", "delhi", "25-06-2023"),
      (4, "rahul", "ranchi", "24-03-2023"),
      (5, "mahesh", "jaipur", "22-03-2023"),
      (6, "prantosh", "kolkata", "18-10-2022"),
      (7, "raman", "patna", "30-12-2022"),
      (8, "prakash", "ranchi", "24-02-2023"),
      (9, "ragini", "kolkata", "03-03-2023"),
      (10, "raushan", "jaipur", "05-02-2023")
    )

    // Create customer DataFrame
    val customerDF = spark.createDataFrame(
      spark.sparkContext.parallelize(customerData.map(Row.fromTuple)),
      customerSchema
    )

    // Define sales schema
    val salesSchema = StructType(Array(
      StructField("customer_id", IntegerType, nullable = true),
      StructField("product_id", IntegerType, nullable = true),
      StructField("quantity", IntegerType, nullable = true),
      StructField("date_of_purchase", StringType, nullable = true)
    ))

    // Sales data
    val salesData = Seq(
      (1, 22, 10, "01-06-2022"),
      (1, 27, 5, "03-02-2023"),
      (2, 5, 3, "01-06-2023"),
      (5, 22, 1, "22-03-2023"),
      (7, 22, 4, "03-02-2023"),
      (9, 5, 6, "03-03-2023"),
      (2, 1, 12, "15-06-2023"),
      (1, 56, 2, "25-06-2023"),
      (5, 12, 5, "15-04-2023"),
      (11, 12, 76, "12-03-2023")
    )

    // Create sales DataFrame
    val salesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(salesData.map(Row.fromTuple)),
      salesSchema
    )

    // Define product schema
    val productSchema = StructType(Array(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("price", IntegerType, nullable = true)
    ))

    // Product data
    val productData = Seq(
      (1, "fanta", 20),
      (2, "dew", 22),
      (5, "sprite", 40),
      (7, "redbull", 100),
      (12, "mazza", 45),
      (22, "coke", 27),
      (25, "limca", 21),
      (27, "pepsi", 14),
      (56, "sting", 10)
    )

    // Create product DataFrame
    val productDF = spark.createDataFrame(
      spark.sparkContext.parallelize(productData.map(Row.fromTuple)),
      productSchema
    )

    //customerDF.show()
    //salesDF.show()

    customerDF
      .join(salesDF,
      customerDF.col("customer_id") === salesDF.col("customer_id"), "outer")
      .show()


    spark.stop()

  }

}
