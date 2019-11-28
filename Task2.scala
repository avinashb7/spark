import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task2 {
	def main(args: Array[String]) = {
	
		val appName = "Task 2"
		val spark = SparkSession.builder.
			master("local").
			appName(appName).
			getOrCreate()
		val sc = spark.sparkContext
		
		// Create a custom class to represent the Customer
		case class Customer(id:Int, name:String, city:String, state:String, zipcode:Int)
		
		try {
			// Read customer.txt store it to RDD
			val filePath = args(0)
			val custRDD = sc.textFile(filePath)
			
			// Create a DataFrame of Customer objects from the RDD by mapping to case class Customer.
			// Convert the RDD to DataFrame
			val custDF = custRDD.map(_.split(",")).map(a => Customer(a(0).trim.toInt,a(1).trim.toString,a(2).trim.toString,a(3).trim.toString,a(4).trim.trim.toInt)).toDF()

			// Register DataFrame as a table.
			custDF.createOrReplaceTempView("customer")

			// Select customer name column
			spark.sql("SELECT name FROM customer").show()
			
		} finally {
			spark.stop()
		}
	}
}