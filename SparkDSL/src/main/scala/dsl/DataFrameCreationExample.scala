package dsl
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import org.apache.spark.sql.SparkSession

object DataFrameCreationExample {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("when_otherwise")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Input data
    val data = Seq(
      ("A", "1-1/1900"),
      ("B", "8\\2/1900"),
      ("C", "5-3/1900"),
      ("D", "5\\4\\1900")
    )

    // Create a DataFrame
    val df = spark.createDataFrame(data).toDF("Names", "DOB")

    // Display the DataFrame
    df.show()
    val rep = df.withColumn("DOB",regexp_replace(col("DOB"),"/","-"))
      .withColumn("DOB",regexp_replace(col("DOB"),"\\\\","-"))

    val spi=rep.withColumn("Day",split(col("DOB"),"-").getItem(0))
      .withColumn("Month",split(col("DOB"),"-").getItem(1))
      .withColumn("Year",split(col("DOB"),"-").getItem(2))

    rep.show()
    spi.show()
    }
}
