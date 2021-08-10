package com.tutorials.azure

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, length, when}

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
    //wasbs is secure, need to import certificate of Azure blob end point URL in the java keystore
    println("Starting to read from Azure Blob...")
    val df = spark.read.option("multiLine","true").json("wasbs://sparkwithazure@sparkazuretutorial.blob.core.windows.net/employee.json")
    println("Reading finished...")
    df.show(truncate = false)

    //Add a new column 'validZipCode' with value as 'true' if length of zipCode is 5, else 'false'
    val newDF = df.withColumn("validZipCode", when(length(col("zipCode"))===5, "true").otherwise("false"))
    newDF.show(truncate = false)

    //Write the new dataframe to Azure Blob with overwrite mode, in parquet format. Capture total time taken to write.
    println("Starting to write...")
    spark.time(newDF.write.format("parquet").mode(SaveMode.Overwrite).save("wasb://sparkwithazure@sparkazuretutorial.blob.core.windows.net/valid-zip-code-data/"))
    println("Write completed...")
  }
}