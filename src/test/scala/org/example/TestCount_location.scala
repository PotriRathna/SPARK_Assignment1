package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest._

class TestCount_location extends FunSuite {

  var sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val lines = sparkSession.read.csv("src/main/resources/user.csv")
  val lines1 = sparkSession.read.csv("src/main/resources/transactions.csv")
  val userColumns = Seq ("userid", "mailid", "language", "country")

  val usertable = lines.toDF (userColumns: _*)
  val transColumns = Seq ("transcationid", "productid", "userid", "price", "productdesc")
  val transtable = lines1.toDF (transColumns: _*)


  assert(Count_location.count(usertable,transtable)=== usertable.join (transtable, usertable ("userid") === transtable ("userid"), "inner")
    .groupBy("country").count().show())
  assert(Count_location.product_bought(transtable)=== transtable.groupBy("userid","productdesc").count() .show ())
  assert(Count_location.Spending_eachuser(transtable)=== transtable.groupBy("userid","productid","productdesc").agg(sum("price")).show())

  sparkSession.stop()

}
