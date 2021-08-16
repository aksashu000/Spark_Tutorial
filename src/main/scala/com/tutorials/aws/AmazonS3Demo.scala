package com.tutorials.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.amazonaws.SDKGlobalConfiguration
import org.apache.spark.sql.functions._

object AmazonS3Demo{
  def main(args: Array[String]): Unit = {
    //Set logger to control log output
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create Spark Session
    val spark = SparkSession.builder.appName("SparkWithS3").master("local[2]").getOrCreate()

    //Get access key and secret key from environment variables
    val FS_S3A_ACCESS_KEY = sys.env("FS_S3A_ACCESS_KEY")
    val FS_S3A_SECRET_KEY = sys.env("FS_S3A_SECRET_KEY")

    //Set AWS S3 endpoint
    val s3EndPoint = "s3.ap-south-1.amazonaws.com"

    //Set Hadoop configurations
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", FS_S3A_ACCESS_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", FS_S3A_SECRET_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3EndPoint)

    println("Starting to read from S3...")
    val employeeDf = spark.read.options(Map("header"-> "true")).csv("s3a://s3tutorial-employee-data/data1.csv")
    println("Finished...")
    employeeDf.show(truncate = false)
    println("Number of partitions: " + employeeDf.rdd.getNumPartitions)

    //Add a new column 'size' with value as 'Large' if quantity > 100, else value will be 'Small'
    val newDF = employeeDf.withColumn("size", when(col("quantity")>=100, "Large").otherwise("Small"))
    newDF.show(truncate = false)

    //Write the new dataframe to S3 with overwrite mode, in parquet format. Capture total time taken to write.
    println("Starting to write...")
    spark.time(newDF.write.format("parquet").mode(SaveMode.Overwrite).save("s3a://s3tutorial-employee-data/output/"))
    println("Write completed...")

    spark.stop()
  }
}

