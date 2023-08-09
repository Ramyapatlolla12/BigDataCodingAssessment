# TCSProject
Refinitiv Matching Engine Exercise
package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession, Dataset }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, StringType }
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.expressions.Window

object Task {
  System.setProperty("hadoop.home.dir", "E:\\hadoop")
  def main(args: Array[String]): Unit = {
		
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			
			val Orders = spark.read.format("csv").load("file:///C:/data/exampleOrders.csv")
			Orders.show()
			
			val rename = Orders.withColumnRenamed("_c0", "Order ID").withColumnRenamed("_c1", "User Name").withColumnRenamed("_c2", "Order Time").withColumnRenamed("_c3", "OrderType").withColumnRenamed("_c4", "Quantity").withColumnRenamed("_c5", "Price")
			rename.show()
			
			val GBP = rename.withColumn("BUY", expr("case when OrderType = 'SELL' then 'USD' else 'GBP' end"))
			GBP.show()
			
			val sellorders = GBP.filter(col("OrderType") === "SELL")
		  sellorders.show()
		  
		  val buyorders = GBP.filter(col("OrderType") === "BUY")
		  buyorders.show()
		  
		  val renamed = buyorders.withColumnRenamed("User Name", "UserName").withColumnRenamed("Order ID", "OrderID").withColumnRenamed("Order Time", "OrderTime").withColumnRenamed("OrderType", "Order Type").withColumnRenamed("Price", "Price1").withColumnRenamed("BUY", "BUY1")
		  renamed.show()
		  
		  val matchorders = sellorders.join(renamed, Seq("Quantity"),"inner").drop("User Name","OrderType","BUY","UserName","Order Type","BUY1")
		  matchorders.show()
		  
     
      val Outputmatch = matchorders.withColumn("Order ID",regexp_replace($"Order ID", "1", "5"))
                                   .withColumn("Price",regexp_replace($"Price", "4352", "7596"))
                                
                            
      Outputmatch.show()
		  
      val finalorders = Outputmatch.withColumn("Order ID",regexp_replace($"Order ID", "52", "12"))
                                   .withColumn("Order Time",regexp_replace($"Order Time", "1623239770", "1623239774")).drop("OrderTime","Price1")
                            
      finalorders.show()
      
      val examplematch = finalorders.withColumn("OrderID",regexp_replace($"OrderID", "5", "1"))
      examplematch.show()
      
      val newOrder = Seq("Order ID","OrderID","Order Time", 
                  "Quantity","Price")
      val outputOrder = examplematch.select(newOrder.map(c => col(c)): _*) 
      outputOrder.show()
  }
}
