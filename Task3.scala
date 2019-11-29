



import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task3 {
	def main(args: Array[String]) = {
	
		val appName = "Task 3"
		val spark = SparkSession.builder.
			.enableHiveSupport()
			.master("local")
			.appName(appName)
			.getOrCreate()
		val sc = spark.sparkContext
		
		try {
					
			val empName = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:/Users/1028109/Downloads/FW__attachment/EmployeeNames.txt")

			val empManager = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:/Users/1028109/Downloads/FW__attachment/EmployeeManagers.txt")

			val empSalary = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:/Users/1028109/Downloads/FW__attachment/EmployeeSalary.txt")

			val empDet = empName.join(empSalary,"empid").join(empManager,"empid")
			empDet.show

			spark.sql("SELECT e.empid,e.empname,e.empsalary,m.empname as managername FROM employee e,employee m WHERE e.empmanager = m.empid").show
						
			/**
			+-----+-------+---------+-----------+
			|empid|empname|empsalary|managername|
			+-----+-------+---------+-----------+
			|    1|    avi|     1000|       john|
			|    2|   nash|     2000|       john|
			|    3|   john|     6000|      rohit|
			|    4| martin|     5000|      rohit|
			|    5|  virat|     4000|      rohit|
			|    6|  rohit|     3000|       john|
			+-----+-------+---------+-----------+
			*/
		} finally {
			spark.stop()
		}
	}
}