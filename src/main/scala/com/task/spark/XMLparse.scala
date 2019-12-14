package com.task.spark

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml.util.XmlFile
import org.apache.spark.sql.functions.explode


object XMLparse {

  def main(args: Array[String]): Unit = {

    val appName = "XMLParse"
    val spark = SparkSession.builder
      .master("local")
      .appName(appName)
      .getOrCreate()

    import spark.implicits._

    try {
      val inputFile = args(0)
      val xmlFile = spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "taxa")
        .load(inputFile)
      val xmlFlat = xmlFile.withColumn("taxon_col", explode($"taxon")).drop($"taxon")
      xmlFlat.show
    } finally {
      spark.stop()
    }
  }
}
