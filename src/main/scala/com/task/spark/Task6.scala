package com.task.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace

object Task6 {
  def main(args: Array[String]) = {

    val appName = "Task 6"
    val spark = SparkSession.builder
      .enableHiveSupport()
      .master("local")
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    try {

      // Load the attached dataset into a dataframe.
      val filePath = args(0)
      val usDF = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filePath)

      // Remove the duplicate rows from the Dataset.
      val withoutDup = usDF.dropDuplicates()
      withoutDup.count

      // Remove the rows having invalid email ids.


      // Remove http:// from the web column
      val webDF = withoutDup.withColumn("web_mod", regexp_replace($"web", "http://", "")).drop($"web")

      // Write the final result to a Hive ORC table (Table should be Partitioned based on state column)

    } finally {
      spark.stop()
    }
  }
}
