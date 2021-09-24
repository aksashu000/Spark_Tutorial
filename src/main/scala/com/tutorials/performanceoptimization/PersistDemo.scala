package com.tutorials.performanceoptimization

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PersistDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("PersistDemo").master("local[2]").getOrCreate()
    val df = spark.read.options(Map("header" -> "true")).csv("D:/Tutorials/Spark/Data/*")
    df.groupBy("housing_median_age").count()
    for(_ <-1 to 3){
      spark.time(df.groupBy("housing_median_age").sum().take(1))
    }

    println("Persist started...")
    //persist the DataFrame
    spark.time(df.persist(StorageLevel.DISK_ONLY).count())
    println("Persist completed...")

    println("Result after persisting...")
    for(_ <-1 to 3){
      spark.time(df.groupBy("housing_median_age").sum().take(1))
    }

    println("Starting removal from cache...")
    //unpersist the DataFrame
    spark.time(df.unpersist())
    println("Removal from cache completed...")

    println("After removing from cache, Running the load again...")
    for(_ <-1 to 3){
      spark.time(df.groupBy("housing_median_age").sum().take(1))
    }
  }
}

