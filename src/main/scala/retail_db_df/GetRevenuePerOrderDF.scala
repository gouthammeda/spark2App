package retail_db_df
import org.apache.spark.sql.SparkSession

/* Created by gouthamkumarreddymeda on 4/13/23 */

object GetRevenuePerOrderDF {
  def main(args:Array[String]):Unit={
    val spark = SparkSession
      .builder()
      .appName("spark sql basic example")
      .master(args(0))
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions","2")

    val orderItems = spark.read.json(args(1))
    val revenuePerOrder = orderItems.groupBy($"order_item_order_id").sum("order_item_subtotal")
    //orderItems.show()
    revenuePerOrder.write.json(args(2))
  }

}
