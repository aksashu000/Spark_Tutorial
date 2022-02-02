package com.tutorials.gcp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, length, when}

object GCSDemo {
  def main(args: Array[String]): Unit = {
    //Set logger to control log output
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create Spark Session
    val spark = SparkSession.builder.appName("SparkWithGCPBucket").master("local[*]").getOrCreate()

    //Set spark conf related to GCP settings
    spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
    spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile", "<complete path to JSON key file>")
    spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    println("Starting to read from GCP Bucket...")
    val df = spark.read.json("gs://<bucket_name>/<read_directory>/employee.json")
    println("Reading finished...")
    df.show(truncate = false)

    //Add a new column 'firstNameLength'
    val newDF = df.withColumn("firstNameLength", length(col("firstName")))
    newDF.show(truncate = false)

    //Write the new dataframe to GCP Bucket with overwrite mode, in parquet format. Capture total time taken to write.
    println("Starting to write...")
    spark.time(newDF.write.format("parquet").mode(SaveMode.Overwrite).save("gs://<bucket_name>/<write_directory>/"))
    println("Write completed...")
  }
}