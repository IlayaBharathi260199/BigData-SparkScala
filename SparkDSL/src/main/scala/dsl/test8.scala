package dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window


// To find "Nth" or secord maximum salary
object test8 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{DataFrame, SparkSession}

    // Create Spark session
    val spark = SparkSession.builder
      .appName("EmployeeDataFrameExample")
      .master("local[*]") // You can change the master URL accordingly
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Define the schema for the DataFrame
    val schema = StructType(Seq(
      StructField("EmployeeID", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = false),
      StructField("Department", StringType, nullable = false),
      StructField("Salary", DoubleType, nullable = false)
    ))

    // Sample data for 10 rows
    val data = Seq(
      Row(1, "John", "HR", 50000.0),
      Row(2, "Alice", "IT", 60000.0),
      Row(3, "Bob", "Finance", 75000.0),
      Row(4, "Eva", "Marketing", 55000.0),
      Row(5, "Charlie", "IT", 70000.0),
      Row(6, "Sophia", "Finance", 80000.0),
      Row(7, "Daniel", "HR", 52000.0),
      Row(7, "Daniel", "HR", 52000.0),
      Row(8, "Grace", "Marketing", 60000.0),
      Row(9, "Henry", "IT", 60000.0),
      Row(10, "Lily", "HR", 48000.0),
      Row(10, "Lily", "HR", 48000.0)
    )

    // Create a DataFrame using the schema and data
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
//    df.show()
//
//    val my_window = Window.partitionBy("Department").orderBy(col("Salary").desc)
//
//    val x = df.withColumn("rank",dense_rank().over(my_window)).show()

//    println(df.count())
//    println(df.distinct().count())
//
//    df.except(df.dropDuplicates()).show(false)


//    df.filter(lower(col("Name")).contains("dan")).show()


    val win = Window.orderBy("salary")
    df.withColumn("rn",dense_rank().over(win)).filter(col("rn") <= 5).select("Name","Department").show()

    df.groupBy("Department").agg(max("salary").as("max_salary")).show(false)

    val df1 = df.select(avg("salary").cast(IntegerType).alias("avg_salary"))
    df.filter(col("salary") > df1.first().getInt(0)).show()  // 59166.666666666664


    df.withColumn("avg",avg("salary").over()).filter(col("salary") > col("avg")).select("Name","Department","Salary").show()

    df.filter(lower(col("Name")).contains("da")).show()
  }
}
