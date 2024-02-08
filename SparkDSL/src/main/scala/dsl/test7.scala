package dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window


//  Task - Find the top 5 customers with the highest number of clicks.
//  The click dataset has columns like 'user_id', 'timestamp', 'interaction_type'.

//  Note:
//  =====
//  limit() will show top 5 result , what if 10 customers occupy first 5 places? I usually prefer to use dense_rank over limit while showing top n results.


object test7 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{SparkSession, DataFrame}

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PivotExample")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Sample data
    val data = Seq(
      ("Alice", "Math", 90),
      ("Alice", "English", 85),
      ("Bob", "Math", 75),
      ("Bob", "English", 80),
      ("Charlie", "Math", 95),
      ("Charlie", "English", 92)
    )

    // Define the schema
    val schema = List("Name", "Subject", "Score")

    // Create DataFrame
    val df: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Show the original DataFrame
    println("Original DataFrame:")
    df.show()

    val windowSpec = Window.orderBy(lit(""))

    df.groupBy("Name").pivot("Subject").max()
      .withColumn("Total",col("English") + col("Math")).withColumn("Id",row_number().over(windowSpec) - 1)
      .show()


   df.withColumn("name_rev",reverse(col("Name"))).show(false)


  }
}
