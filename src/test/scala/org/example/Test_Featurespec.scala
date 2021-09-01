package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
class Test_Featurespec extends FeatureSpec with GivenWhenThen {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._
  info("Read CSV File")
  info("spending done by each user on each product")
  feature("Read csv") {
    scenario("GroupBy") {
      Given("Count Products")
      val TransData = List(("1001","456","12000"), ("1002","123","45000"), ("1003", "111","6400"), ("1004", "123","23000"), ("1005", "324","23000"),
        ("1006", "111","3400")).toDF("productid", "userid","price")
      When("DF ")
      val Spendingby_eachuser= Service.Spending_eachuser(TransData)
      val result = List(("123", "1002",45000.0), ("456","1001",12000.0),("111","1006",3400.0),("111","1003",6400.0),("324","1005",23000.0),("123","1004",23000.0)).toDF("User","productid","sum(price)")
      Then("we get correct result")
      Spendingby_eachuser.collect() should contain allElementsOf result.collect()
    }
  }
  sparkSession.stop()
}
