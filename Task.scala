package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession, Dataset }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, StringType }
import org.apache.spark.sql.{SparkSession, DataFrame}
object task {
   def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/data/Hadoop")
    println("====started==")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
   
    val schema = StructType(Seq(
      StructField("OrderID", IntegerType, nullable = true),
      StructField("UserName", StringType, nullable = true),
      StructField("OrderTime", LongType, nullable = true),
      StructField("OrderType", StringType, nullable = true),
      StructField("Quantity", IntegerType, nullable = true),
      StructField("Price", IntegerType, nullable = true)))
       
       val df = spark.read.option("Header", "false").schema(schema).csv("file:///C:/data/exampleOrders.csv")
       df.show()
       val ordersWithTimestamp = df.withColumn("OrderTimestamp", $"OrderTime".cast("timestamp"))
       
       val buyOrders = ordersWithTimestamp.filter(col("OrderType") === "BUY").withColumnRenamed("Price", "BuyPrice")
           buyOrders.show()
        val sellOrder = ordersWithTimestamp.filter(col("OrderType") === "SELL").withColumnRenamed("Price", "SellPrice")
        sellOrder.show()
        
        val sellOrders = sellOrder.withColumnRenamed("OrderID", "OrderID_x").withColumnRenamed("UserName", "UserName_x")
                                   .withColumnRenamed("OrderTime", "OrderTime_x").withColumnRenamed("OrderType", "OrderType_x")
                                   .withColumnRenamed("OrderTimestamp", "OrderTimestamp_x")
        val matchedOrders = buyOrders.join(sellOrders,Seq("Quantity"),"inner")
                                      .withColumn("MatchedTime", least(col("OrderTimestamp"), col("OrderTimestamp_x")))
                                      .withColumn("MatchedOrderID_BUY", col("OrderID"))
                                      .withColumn("MatchedOrderID_SELL", col("OrderID_x"))
                                      .withColumn("MatchedPrice", col("SellPrice"))
               matchedOrders.show()                       
           val finalResult = matchedOrders.select("MatchedOrderID_BUY", "MatchedOrderID_SELL", "MatchedPrice", "Quantity", "MatchedTime")
                                       .drop("OrderTimestamp_x")
        finalResult.show()
       
        
   }
  
}