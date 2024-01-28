package dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/*
 Task - The dataset contains information about food items in bills.
 Your assignment is to determine the frequency of each food item ordered.
*/
object test3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("YourAppName").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = Seq(
      (101, Array("dosa", "biriyani", "idli")),
      (102, Array("biriyani", "mineral water")),
      (103, Array("rice", "mineral water", "poha")),
      (109, Array("idli", "biriyani", "poha"))
    )

    val schema = StructType(Seq(
      StructField("Student_id", IntegerType, nullable = false),
      StructField("Items", ArrayType(StringType), nullable = false)
    ))

    val rows = data.map { case (id, items) => Row(id, items) }

    val input_df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    input_df.show(false)

    input_df.select(explode(col("Items")).alias("item")).groupBy("Item").count().orderBy(col("count").desc).show()
  }
}
