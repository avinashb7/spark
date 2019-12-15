package com.task.spark

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml.util.XmlFile
import org.apache.spark.sql.functions.explode

object XMLparse {

  def main(args: Array[String]): Unit = {

    //Setting app name.
    val appName = "XMLParse"
    
    //Create spark session.
    val spark = SparkSession.builder
      .master("local")
      .appName(appName)
      .getOrCreate()

    //Import implicits to use "$" instead of column names.
    import spark.implicits._

    try {
      //Read input file as 1st arguement in spark-submit command
      val inputFile = args(0)
      
      //Read the data using databricks package, import the package and add it in pom.xml
      val xmlFile = spark.read.format("com.databricks.spark.xml")
                    .option("rowTag", "taxa")
                    .load(inputFile)
      
      //Verify the Schema
      xmlFile.printSchema
      /*
      root
       |-- _id: string (nullable = true)
       |-- taxon: array (nullable = true)
       |    |-- element: struct (containsNull = true)
       |    |    |-- _VALUE: string (nullable = true)
       |    |    |-- _id: string (nullable = true)
       |    |    |-- _idref: string (nullable = true) 
      */
      
      // Print the result to verify the output
      xmlFile.show(5)
      /*
        +-------------------+--------------------+
        |                _id|               taxon|
        +-------------------+--------------------+
        |               taxa|[[, D_asper,], [,...|
        |arizonae mojavensis|[[,, D_arizonae],...|
        |            ingroup|[[,, D_a_american...|
        |        montana sbg|[[,, D_borealise]...|
        |        picticornis|[[,, D_cyrtoloma]...|
        +-------------------+--------------------+
      */
      
      //Flatten the "taxon" column using explode and drop the original column if required.
      val xmlFlat = xmlFile.withColumn("taxon_col",explode($"taxon._id"))
                    .drop($"taxon")

      xmlFlat.show(5)
      /* Final output
        +----+------------+
        | _id|   taxon_col|
        +----+------------+
        |taxa|     D_asper|
        |taxa|    D_daruma|
        |taxa|D_latifshahi|
        |taxa|  D_hirtipes|
        |taxa|D_polychaeta|
        +----+------------+
      */

      // To check the distinct values in the column.
      val distinctID = xmlFlat.select($"_id").distinct()
      distinctID.show()
      /*
        +-------------------+
        |                _id|
        +-------------------+
        |            ingroup|
        |         virilis gr|
        |arizonae mojavensis|
        |               taxa|
        |        virilis sbg|
        |        picticornis|
        |        montana sbg|
          +-------------------+
      */
      
      //Write to any file or database if required.
      
    } finally {
      spark.stop()
    }
  }
}
