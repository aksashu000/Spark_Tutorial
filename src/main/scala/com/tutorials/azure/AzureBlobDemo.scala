package com.tutorials.azure

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AzureBlobDemo {
  def main(args: Array[String]): Unit = {
    //Set logger to control log output
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create Spark Session
    val spark = SparkSession.builder.appName("SparkWithAzureBlob").master("local[*]").getOrCreate()

    //Get the value of Azure access key from the environment variable
    val ACCESS_KEY = sys.env("AZURE_ACCESS_KEY")

    //Set spark conf -> Access Key for Azure Blob Storage Account
    spark.conf.set("fs.azure.account.key.sparkazuretutorial.blob.core.windows.net", ACCESS_KEY)

    //Read the data from "wasb" file system -> Windows Azure Storage Blob
    val df = spark.read.option("multiLine","true").json("wasb://sparkwithazure@sparkazuretutorial.blob.core.windows.net/*")
    df.show()
  }
}
