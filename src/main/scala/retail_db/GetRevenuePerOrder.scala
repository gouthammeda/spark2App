package retail_db
import org.apache.spark.{SparkConf,SparkContext}

object GetRevenuePerOrder {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get revenue per order")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orderItems = sc.textFile(args(1))
    orderItems.take(3).foreach(println)
    val revenuePerOrder = orderItems.map(oi=>(oi.split(",")(1).toInt,oi.split(",")(4).toFloat)).reduceByKey(_+_)
      //it is converting array of tuples or key value pairs or pair rdd's into line of string
      .map(oi=>oi._1+","+oi._2)
    //revenuePerOrder.saveAsTextFile(args(2))
    revenuePerOrder.take(5).foreach(println)
  }

}


