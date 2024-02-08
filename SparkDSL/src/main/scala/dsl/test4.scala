package dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

//Identifying missing values and treating/imputing missing values is an important step in exploratory data analysis. Let's try it in Spark dataframe API.
//   a) Identify columns that contain no null values.
//   b) Identify columns where every value is null.
//   c) Identify columns with at least one null value

object test4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("YourAppName").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val emp_data = Seq(
      Row(1, "Neha", 30, null, "IT"),
      Row(2, "Mark", null, null, "HR"),
      Row(3, "David", 25, null, "HR"),
      Row(4, "Carol", 30, null, null)
    )

    // Define the schema for the DataFrame
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = true),
        StructField("salary", LongType, nullable = true),
        StructField("department", StringType, nullable = true)
      )
    )

    // Create the DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(emp_data), schema)

    // Show the DataFrame
    df.show()

   //a) Identify columns that contain  no null values.
    println("==NotNull==")
    val not_null = df.columns.filter(x => df.filter(col(x).isNull).count() == 0)
    not_null.foreach(println)

    //b) Identify columns where every value is null.
    println("==Null==")
    val _null = df.columns.filter(x => df.filter(col(x).isNotNull).count() == 0)
    _null.foreach(println)

    //c) Identify columns with at least one null value
    println("==OneNull==")
    val one_null = df.columns.filter(x => df.filter(col(x).isNull).count() == 1)
    one_null.foreach(println)




  }
}
