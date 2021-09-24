package com.tutorials.performanceoptimization

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CacheDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("CacheDemo").master("local[2]").getOrCreate()
    val df = spark.read.options(Map("header" -> "true")).csv("D:/Tutorials/Spark/Data/*") //624 MB

    for(_ <-1 to 3){
      spark.time(df.groupBy("housing_median_age").sum().take(1))
    }

    println("Caching started...")
    //cache the DataFrame
    spark.time(df.cache().count())
    println("Caching completed...")

    println("Result after caching...")
    for(_ <-1 to 3){
      spark.time(df.groupBy("housing_median_age").sum().take(1))
    }

    println("Starting cache removal...")
    //unpersist the DataFrame
    spark.time(df.unpersist(true))
    println("Removal from cache completed...")

    println("After removing from cache, Running the load again...")
    for(_ <-1 to 3){
      spark.time(df.groupBy("housing_median_age").sum().take(1))
    }

  }
}

