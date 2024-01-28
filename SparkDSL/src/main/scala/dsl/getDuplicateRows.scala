package dsl

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object getDuplicateRows {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("FindDuplicateRows")
      .master("local[*]") // You can change the master URL accordingly
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    // Sample DataFrame with some duplicate rows
    val data = Seq(
      ("John", 28, "HR"),
      ("Alice", 22, "IT"),
      ("Bob", 35, "Finance"),
      ("John", 28, "HR"), // Duplicate row
      ("Charlie", 30, "Marketing"),
      ("Alice", 22, "IT"), // Duplicate row
      ("Daniel", 32, "Finance")
    )

    val columns = Seq("Name", "Age", "Department")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    df.show(false)

    // Find and display duplicate rows
    df.except(df.distinct()).show(false)


  }
}
