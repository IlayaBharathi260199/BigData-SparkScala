package dsl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


//  Task - Find the top 5 customers with the highest number of clicks.
//  The click dataset has columns like 'user_id', 'timestamp', 'interaction_type'.

//  Note:
//  =====
//  limit() will show top 5 result , what if 10 customers occupy first 5 places? I usually prefer to use dense_rank over limit while showing top n results.


object test6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("YourAppName").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Define the schema for the DataFrame
    val schema = StructType(
      Array(
        StructField("ROLL_NO", IntegerType, true),
        StructField("SUBJECT", StringType, true),
        StructField("MARKS", IntegerType, true)
      )
    )

    // Create an RDD of Rows
    val data = Seq(
      Row(1001, "English", 84),
      Row(1001, "Physics", 55),
      Row(1001, "Maths", 45),
      Row(1001, "Science", 35),
      Row(1001, "History", 32),
      Row(1002, "English", 84),
      Row(1002, "Physics", 62),
      Row(1002, "Maths", 78),
      Row(1002, "Science", 96),
      Row(1002, "History", 32)
    )

    // Create an RDD using the schema and data
    val rdd = spark.sparkContext.parallelize(data)

    // Create the DataFrame
    val df = spark.createDataFrame(rdd, schema)

    // Show the DataFrame
    df.show()

    // Approach 1
    df.groupBy("ROLL_NO").pivot("SUBJECT").max("MARKS")
      .withColumn("Total_marks", col("English") + col("History") + col("Maths") + col("Physics") + col("Science"))
      .show(false)


    // Approach 2
    val df1 = df.groupBy("ROLL_NO").agg(sum("MARKS"))
    val df2 = df.groupBy("ROLL_NO").pivot("SUBJECT").max("MARKS")
    df1.join(df2, Seq("ROLL_NO")).show(false)

    val df3 = df.groupBy("ROLL_NO").pivot("SUBJECT").sum("MARKS").show()


  }
}
