package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.example.Count_location._
import org.scalatest._

class TestCount_location extends FunSuite {

  var sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val lines = read1(sparkSession.read.csv("src/main/resources/user.csv"))
  val lines1 = read2(sparkSession.read.csv("src/main/resources/transactions.csv"))

  assert(Count_location.count(lines,lines1)=== lines.join (lines1, lines ("userid") === lines1("userid"), "inner")
    .groupBy("country").count().show())
  sparkSession.stop()

}
