import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task5 {
	def main(args: Array[String]) = {
	
		val appName = "Task 5"
		val spark = SparkSession.builder.
			master("local").
			appName(appName).
			getOrCreate()
		val sc = spark.sparkContext
		
		try {
		
			val filePath = args(0)
			val myLog = sc.textFile("filePath")

			// Count the number of words in the file.
			val totalWords = myLog.flatMap(_.split("""\s+""")).count()
			println(totalWords)

			// Count the number of line in the file.
			val totalLines = myLog.count
			println(totalLines)
			
			// Count the number of lines having log level as “ERROR”
			val errorCount = myLog.filter(_.contains("ERROR")).count
			println(errorCount)

			// Count the number of words starting with “com.apple.”
			val comApp = myLog.flatMap(_.split("""\s+""")).filter(_.startsWith("com.apple")).count()
			println(comApp)

			// Create different files for Warning, error and INFo logs in HDFS. 
			val hdfsPATH = "/user/training/log"
			
			val errorLog = myLog.filter(_.contains("ERROR"))
			errorLog.saveAsTextFile(hdfsPATH + "/error")
			
			val warnLog = myLog.filter(_.contains("WARN"))
			warnLog.saveAsTextFile(hdfsPATH + "/warning")
			
			val infoLog = myLog.filter(_.contains("INFO"))
			infoLog.saveAsTextFile(hdfsPATH + "/info")
			
		} finally {
			spark.stop()
		}
	}
}
