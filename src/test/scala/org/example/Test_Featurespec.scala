package org.example
import org.apache.spark.sql.SparkSession
import org.scalatest._
class Test_Featurespec extends FeatureSpec with GivenWhenThen {
  var sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  info("Read CSV File")
  info("products bought by each user")
  feature("Read csv") {
    scenario("GroupBy") {
      Given("Count Products")
      val lines1 = Count_location.read2(sparkSession.read.csv("src/main/resources/transactions.csv"))
      When("CSV to DF ")
      val result= Count_location.product_bought(lines1)
      Then("we get correct result")
      assert(result == lines1.groupBy("userid","productdesc").count() .show ())
    }
  }

}
