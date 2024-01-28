package dsl

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



// To find "Nth" or secord maximum salary
object test9 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("UserActionDataFrameExample")
      .master("local[*]") // You can change the master URL accordingly
      .getOrCreate()

    // Define the schema for the DataFrame
    val schema = StructType(Seq(
      StructField("UserID", StringType, nullable = false),
      StructField("Action", StringType, nullable = false),
      StructField("Timestamp", StringType, nullable = false)
    ))

    // Sample data
    val data = Seq(
      Row("u10", "chatting", "7:00AM"),
      Row("u1", "logout", "8:30AM"),
      Row("u1", "login", "9:01AM"),
      Row("u1", "browsing", "9:02AM"),
      Row("u10", "logout", "9:02AM"),
      Row("u2", "login", "9:02AM"),
      Row("u1", "login", "10:30PM")
    )

    // Create a DataFrame using the schema and data
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
    df.show(false)
    df.filter(col("Action").isin("logout","login")). orderBy("UserID").show(false)

  }
}
