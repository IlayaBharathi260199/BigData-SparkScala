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


object test5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("YourAppName").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data_tuples = Seq(
      (1, "2023-01-01 12:00:00", "click"),
      (2, "2023-01-01 12:05:00", "view"),
      (1, "2023-01-01 12:10:00", "click"),
      (3, "2023-01-01 12:15:00", "view"),
      (2, "2023-01-01 12:20:00", "click"),
      (1, "2023-01-01 12:25:00", "view"),
      (3, "2023-01-01 12:30:00", "click"),
      (3, "2023-01-01 12:30:00", "click"),
      (2, "2023-01-01 12:35:00", "view"),
      (1, "2023-01-01 12:40:00", "click"),
      (3, "2023-01-01 12:45:00", "view"),
      (1, "2023-01-02 12:10:00", "click"),
      (1, "2023-01-03 12:10:00", "click"),
      (1, "2023-01-04 12:10:00", "view")
    )

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("action", StringType, nullable = false)
      )
    )

    val rows = data_tuples.map { case (id, timestamp, action) =>
      Row(id, timestamp, action)
    }

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    // Show the DataFrame
    df.show()

    df.withColumn("day",dayofmonth(col("timestamp")) === 1).filter(col("day") === "true")
      .select("id","action").show()

    val my_window = Window.orderBy(col("max_count").desc)
    df.withColumn("Count", when(col("action") === "click", 1).otherwise(0))
      .drop("timestamp")
      .filter(col("action") === "click")
      .groupBy("id", "action").agg(sum(col("Count")).alias("max_count"))
      .withColumn("rank", dense_rank().over(my_window))
      .filter(col("rank") <= 2)
      .show(false)

    val my_window1 = Window.orderBy(col("count").desc)
    df.filter(col("action") === "click")
      .groupBy("id","action").count()
      .withColumn("rank", dense_rank.over(my_window1))
      .filter(col("rank") <= 5)
      .show(false)

val myw = Window.partitionBy("action").orderBy(col("count").desc)
    df.groupBy("id","action").count().filter(col("action") === "click")
     .withColumn("Rank",dense_rank().over(myw))
      .filter(col("Rank") <= 5).show(false)



    df.filter(col("action").isin("click","view")).show(false)

  }
}
