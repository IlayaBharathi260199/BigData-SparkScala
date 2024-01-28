package dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/*
Task - we have a dataset representing books issued by students,
and we want to aggregate the books for each student. Multiple books should be shown separated by ";"
*/
object test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("YourAppName").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val book_issued = Seq(
      (101, "Mark", "White Tiger"),
      (102, "Ria", "The Fountainhead"),
      (102, "Ria", "The Secret History"),
      (101, "Mark", "Bhagavad Gita"),
      (103, "Loi", "The Fountainhead")
    )

    val schema = StructType(Seq(
      StructField("Student_id", IntegerType, nullable = false),
      StructField("Student_name", StringType, nullable = false),
      StructField("Book_issued", StringType, nullable = false)
    ))

    val rows = book_issued.map(data => Row(data._1, data._2, data._3))

    val input_df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    input_df.groupBy("Student_id","Student_name").agg(concat_ws(";",collect_list("Book_issued")).alias("books")).show(false)

  }
}
