package retail_db_df


/* Created by gouthamkumarreddymeda on 5/17/23
*  problem statement: Get top n products per day by revenue
*  consider only complete and closed orders
*  output: order_Date,product_name, revenue
*  selection or projection
*  filter the data set
*  aggregations - sum,avg,min,max
*  sorting and Ranking
*  */

object GetTopNProductsPerDayDF {

  import org.apache.spark.sql.SparkSession

    def main(args:Array[String]): Unit ={
            val spark = SparkSession
              .builder
              .appName("Get Top N Products Per Day using Dataframe Operations")
              .master(args(0))
              .getOrCreate()
            import spark.implicits._
            spark.sparkContext.setLogLevel("ERROR")
            spark.conf.set("spark.sql.shuffle.operations","2")
      val inputBaseDir = args(1)
      val ordersDF = spark.read.json(inputBaseDir + "/orders")
      ordersDF.show()
    }
}
