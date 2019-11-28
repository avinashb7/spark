import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task4 {
	def main(args: Array[String]) = {
	
		val appName = "Task 4"
		val spark = SparkSession.builder.
			master("local").
			appName(appName).
			getOrCreate()
		val sc = spark.sparkContext
		
		try {
					
			// You have been given below list in scala (name,sex,cost) for each work done.
			// List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
			// Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key)
			
			val myList = List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
			val myRDD = sc.parallelize(myList.map {
				case(name,sex,cost) => ((name,sex),cost)
			}).reduceByKey(_ + _)
			myRDD.foreach(println)
		} finally {
			spark.stop()
		}
	}
}