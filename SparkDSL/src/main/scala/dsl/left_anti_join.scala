package dsl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/*
Left_Anti_Join(DataFrame API)---> will return all the records from left table which do not have matching key from right table
                                                            (or)
                     will return unmatched records from left table based join column
 */
object left_anti_join {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Left-Anti-Join")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val leftData = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie"))
    val rightData = Seq((1, "Engineer"), (3, "Designer"))

    val leftDF: DataFrame = spark.createDataFrame(leftData).toDF("id", "name")
    val rightDF: DataFrame = spark.createDataFrame(rightData).toDF("id", "occupation")

    // Show the created DataFrames
    println("Left DataFrame:")
    leftDF.show()

    println("Right DataFrame:")
    rightDF.show()


    println("Left")
    val left = leftDF.join(rightDF,Seq("id"),"left").show(false)

    println("Left_anti")
    val left_Anti = leftDF.join(rightDF,Seq("id"),"left_anti").show(false)

  }
}
