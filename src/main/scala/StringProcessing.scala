object StringProcessing {
  def main(args:Array[String])={
    val o = "10,2013-07-25 00:00:00.0,11599,CLOSED"

    //Get date using substring and indexOf
    val firstComma = o.indexOf(",")
    println("Index of first, is " + firstComma)

    val secondComma = o.indexOf(",",firstComma+1)
    println("Index of second, is" + secondComma)
    println("Date using substring "+ o.substring(firstComma + 1,secondComma))
    //Get date using split
    //val oArray: Array[String] = o.split(",")
    //oArray.foreach(println)
    val orderDate = o.split(",")(1)
    println("Date using split and index "+ orderDate)

    //Get order_id and order_customer_id as integers
    val orderId = o.split(",")(0).toInt
    val orderCustomerId = o.split(",")(2).toInt
    println("order id is " + orderId)
    println("order customer id is "+orderCustomerId)

    // compare whether order_Status is CLOSED OR not
    val orderStatus = o.split(",")(3)
    val isClosed = orderStatus == "CLOSED"
    println("Is status of order id "+orderId+" closed? "+ isClosed)

    // Get date as integer in the format of YYYYMMDD
    val orderDateAsInt = orderDate.substring(0,10).replace("-","").toInt
    println("order date in YYYYMMDD format is "+ orderDateAsInt)
  }
}
