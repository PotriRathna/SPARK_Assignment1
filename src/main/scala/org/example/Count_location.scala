/*
a)	Count of unique locations where each product is sold.
b)	Find out products bought by each user.
c)	Total spending done by each user on each product.
 */
package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object Count_location extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Jointable")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val userdf = Service.readDF("src/main/resources/user.csv",Seq ("userid", "mailid", "language", "country"))
    val transdf = Service.readDF("src/main/resources/transactions.csv",Seq ("transcationid", "productid", "userid", "price", "productdesc"))

    println(s"Count of unique locations where each product is sold ${Service.count(userdf,transdf).show()}" )
    println(s"Find out products bought by each user ${ Service.product_bought(transdf).show()}")
    println(s"Total spending done by each user on each product ${Service.Spending_eachuser(transdf).show()}")
}
