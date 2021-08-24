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
    val lines1 = Count_location.read2(sparkSession.read.csv("src/main/resources/transactions.csv"))
    test(" spending done by each user on each product"){
    assert(Count_location.Spending_eachuser(lines1)=== lines1.groupBy("userid","productid","productdesc").agg(sum("price")).show())}
  }
  after {
    sparkSession.stop()
  }
}
