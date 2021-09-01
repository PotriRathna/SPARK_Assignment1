package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestCount_location extends FunSuite {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
 import sparkSession.implicits._
  val UserData= List(("123","India"),("324","London"),("456","Stockholm")
             ,("765","India"),("567","Denmark"),("111","Stockholm")).toDF("userid","country")
  val TransData=List(("456"),("123"),("111"),("123"),("324"),("111"),
             ("567"),("765"),("765"),("123"),("324")).toDF("userid")
  val count_location = Service.count(UserData,TransData)
  val result = List(("Stockholm",3),("London",2),("India",5),("Denmark",1)).toDF("Location","count")

  count_location.collect() should contain allElementsOf result.collect()

    sparkSession.stop()

}