import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task1 {
	def main(args: Array[String]) = {
	
		val appName = "Task 1"
		val spark = SparkSession.builder.
			master("local").
			appName(appName).
			getOrCreate()
		val sc = spark.sparkContext
		
		try {
			// Create a RDD without reading a FILE
			val myList = List(1,2,3,4,5)
			val myRDD = sc.parallelize(myList)
			myRDD.take(5)
			
			// Create a RDD reading bank.csv (attached) FILE (comma separated)
			val filePath = args(0)
			val bankRDD = sc.textFile(filePath)
			
			// Split each record into tokens/fields
			val splitRDD = bankRDD.map(_.split(";"))
			
			// Find total number of fields in the record
			val bankDF = spark.read.format("csv").option("delimiter",";").option("inferSchema","true").option("header","true").load(("/FileStore/tables/bank.csv"))
			println(bankDF.columns.size)
			bankDF.show
			
			// Total number of customers who got credit and who could not get credit.
			
			// Find how many people are married, employed and educated who got the loan and who could not get the loan.
			
		} finally {
			spark.stop()
		}
	}
}
