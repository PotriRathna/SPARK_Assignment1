package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest._
class TestBefore_andAfter extends FunSuite with BeforeAndAfter{
  var sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  before {
    val lines1 = sparkSession.read.csv("src/main/resources/transactions.csv")
    val transColumns = Seq ("transcationid", "productid", "userid", "price", "productdesc")
    val transtable = lines1.toDF (transColumns: _*)
  test(" spending done by each user on each product"){
    assert(Count_location.Spending_eachuser(transtable)=== transtable.groupBy("userid","productid","productdesc").agg(sum("price")).show())}
  }
  after {
    sparkSession.stop()
  }
}
