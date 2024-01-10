package Pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AWS_S3_Spark_AWS_S3 {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("AWS_S3_Spark_AWS_S3")
      .config("fs.s3a.access.key", "Your_AccessKey")
      .config("fs.s3a.secret.key", "Your_SecretKey")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")


    //1.Reading from AWS S3
    val csv = spark.read.format("csv").option("header", "true").load("s3a://ilaya2/dt.csv")
    csv.show()


    //2.===========Transformations===========
    val mod = csv.filter(col("amount") > 100)
    mod.show(false)

    //3.Writing to AWS_S3 as CSV
    mod.write.mode("overwrite").csv("s3a://ilaya2/dtcsv")


  }

}
