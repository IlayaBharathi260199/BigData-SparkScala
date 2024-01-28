package dsl

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 TASK: Extract the first and last names from these emails,
       which are structured with dots as delimiters.
 */
object test10 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("EmployeeDataFrameExample")
      .master("local[*]") // You can change the master URL accordingly
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(Seq(
      StructField("email", StringType, nullable = false)
    ))

    // Create an RDD with the data
    val data = Seq(
      Row("john.doe@email.com"),
      Row("jane.smith@email.com"),
      Row("alice.wonder@email.com")
    )

    // Create the DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
    df.show(false)

    df.withColumn("full_name",split(col("email"),"@").getItem(0))
      .withColumn("first_name",split(col("full_name"),"\\.").getItem(0))
      .withColumn("last_name",split(col("full_name"),"\\.").getItem(1))
      .drop("full_name").show(false)
  }
}
