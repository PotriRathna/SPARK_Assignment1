package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
class TestBefore_andAfter extends FunSuite with BeforeAndAfter{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._
  before {

    val TransData = List(("1001", "456"), ("1002", "123"), ("1003", "111"), ("1004", "123"), ("1005", "324"), ("1006", "111"),
      ("1007", "567"), ("1008", "765"), ("1009", "765"), ("1010", "123"), ("1011", "324")).toDF("productid", "userid")

    test(" products bought by each user") {
      val count_product = Service.product_bought(TransData)
      val result = List(("456", 1), ("111", 2), ("567", 1), ("123", 3), ("765", 2), ("324", 2)).toDF("User", "count")
      count_product.collect() should contain allElementsOf result.collect()
    }
  }
  after {
    sparkSession.stop()
  }
}
