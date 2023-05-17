package retail_db
import org.apache.spark.{SparkConf,SparkContext}
/* Created by gouthamkumarreddymeda on 11/2/22
* Problem Statement: Get top n products per day by revenue
* considering only COMPLETE and CLOSED orders
* Output: order_date,product_name,revenue
* */

object GetTopNProductsPerDay {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get revenue per order")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Reading Data(from file system or database)
    //Read orders,order_items and products
    val inputBaseDir = args(1)
    val orders = sc.textFile(inputBaseDir+"/orders")
    val orderItems = sc.textFile(inputBaseDir+"/order_items")
    val products = sc.textFile(inputBaseDir+"/products")

    // Row level transformation
    // Orders - filter and apply map to transform data
    // "1,2013-07-25 00:00:00.0,11599,CLOSED" -> (1,"2013-07-25 00:00:00.0")
    val ordersMap = orders
      .filter(e=>List("COMPLETE","CLOSED").contains(e.split(",")(3)))
      .map(e => (e.split(",")(0).toInt,e.split(",")(1)))
    ordersMap.take(10).foreach(println)

    //order_items - use map to transform the data.
   // "1,1,957,1,299.98,299.98" -> (1,(957,299.98))
   val orderItemsMap = orderItems.map(e=>(e.split(",")(1).toInt,(e.split(",")(2).toInt,e.split(",")(4).toFloat)))
    orderItemsMap.take(10).foreach(println)

    val regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val productsMap = products.
      map(e=>(e.split(regex,-1)(0).toInt,e.split(regex,-1)(2)))
    productsMap.take(10).foreach(println)
  }
}
