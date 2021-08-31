package org.example
/*
a)	Count of unique locations where each product is sold.
b)	Find out products bought by each user.
c)	Total spending done by each user on each product.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
object Count_location {

  // a).  Count of unique locations where each product is sold.
  def count(x:DataFrame,y:DataFrame):Any = x.join (y, x ("userid") === y ("userid"), "inner")
      .groupBy("country").count()

  // b).  Find out products bought by each user.
  def product_bought(y:DataFrame):Any = y.select("productid", "userid")
      .groupBy("userid").count()

  // c)	Total spending done by each user on each product.
  def Spending_eachuser(Y:DataFrame):Any= Y.groupBy("userid","productid","productdesc")
    .agg(sum("price"))

  def readfile(file:DataFrame,col:Seq[String]):DataFrame= file.toDF(col:_*)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc1: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Jointable")
      .getOrCreate()
    sc1.sparkContext.setLogLevel("ERROR")

    val lines = readfile(sc1.read.csv("src/main/resources/user.csv"),Seq ("userid", "mailid", "language", "country"))
    val lines1 = readfile(sc1.read.csv("src/main/resources/transactions.csv"),Seq ("transcationid", "productid", "userid", "price", "productdesc"))
    println(s"Count of unique locations where each product is sold")
    count(lines,lines1)
    println(s"Find out products bought by each user")
    product_bought(lines1)
    println(s"Total spending done by each user on each product")
    Spending_eachuser(lines1)
  }
}
