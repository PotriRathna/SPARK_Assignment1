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

  def count(x:DataFrame,y:DataFrame):Any =
  {
    // a).  Count of unique locations where each product is sold.
    x.join (y, x ("userid") === y ("userid"), "inner")
      .groupBy("country").count() .show ()
  }
  def product_bought(y:DataFrame):Any =
  {
    // b).  Find out products bought by each user.

      y.groupBy("userid","productdesc").count() .show ()
  }
  def Spending_eachuser(Y:DataFrame):Any=
    {
     // c)	Total spending done by each user on each product.
      Y.groupBy("userid","productid","productdesc").agg(sum("price")).show()
    }
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc1: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Jointable")
      .getOrCreate()
    sc1.sparkContext.setLogLevel("ERROR")

    val lines = sc1.read.csv("src/main/resources/user.csv")
    val lines1 = sc1.read.csv("src/main/resources/transactions.csv")
    val userColumns = Seq ("userid", "mailid", "language", "country")
    val usertable = lines.toDF (userColumns: _*)
    val transColumns = Seq ("transcationid", "productid", "userid", "price", "productdesc")
    val transtable = lines1.toDF (transColumns: _*)
    println(s"Count of unique locations where each product is sold ${count(usertable,transtable)}")
    println(s"Find out products bought by each user ${product_bought(transtable)}")
    println(s"Total spending done by each user on each product ${Spending_eachuser(transtable)}")
  }
}
