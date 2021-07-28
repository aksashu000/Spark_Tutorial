package com.tutorials.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.amazonaws.SDKGlobalConfiguration
import org.apache.spark.sql.functions._

object AmazonS3Demo{
  def main(args: Array[String]): Unit = {
    //Set logger to control log output
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Set the property to use V4 signature of S3
    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    //Create Spark Session
    val spark = SparkSession.builder.appName("SparkWithS3").master("local[*]").getOrCreate()

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
    val employeeDf = spark.read.option("multiLine","true").json("s3a://s3tutorial-employee-data/employee.json")
    println("Finished...")
    employeeDf.show(truncate = false)

    //Add a new column 'validZipCode' with value as 'true' if length of zipCode is 5, else 'false'
    val newDF = employeeDf.withColumn("validZipCode", when(length(col("zipCode"))===5, "true").otherwise("false"))
    newDF.show(truncate = false)

    //Write the new dataframe to S3 with overwrite mode, in parquet format. Capture total time taken to write.
    println("Starting to write...")
    spark.time(newDF.write.format("parquet").mode(SaveMode.Overwrite).save("s3a://s3tutorial-employee-data/valid-zip-code-data/"))
    println("Write completed...")

    spark.stop()
  }
}

