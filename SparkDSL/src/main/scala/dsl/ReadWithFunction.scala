package dsl

import Custom_Functions.CustomReadFuction._
import org.apache.spark.sql.SparkSession




object ReadWithFunction {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Spark_SQL")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

//    val Parqdata = parquet(spark, "/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/customer")
//    Parqdata.show(false)

    val csvdata = csv(spark,"C:\\Users\\ILAYA BHARATHI M\\Desktop\\Pandas data\\Cricket_Data_final2.csv")
    csvdata.show(false)


  }

}
