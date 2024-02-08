import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("YourAppName").master("local").getOrCreate()

    val complex_data = Seq(
      "   Hello, World! This is the first sentence--a. Another--sentence follows.  ",
      "PySpark is awesome. It allows for distributed data processing. Exciting \u0000 stuff!",
      "Let's analyze text data. We can use various transformations-and \u0000 and actions in PySpark.",
      "Data processing is crucial for extracting valuable insights \u0000 from large datasets. " +
        "Spark provides powerful tools for--this purpose."
    )

    val schema = StructType(Seq(StructField("raw_text", StringType, nullable = true)))
    val rows = complex_data.map(data => Row(data))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    //df.show(false)
val df1 =df.withColumn("raw_text",regexp_replace(col("raw_text"),"[-\\-!'.,]",""))
  val df2 = df1.withColumn("raw_text",split(col("raw_text")," "))
    val df3 = df2.select(explode(col("raw_text"))).groupBy("col").count()
    df3.show(100,false)


  }
}
