package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.example.Count_location._
import org.scalatest._

class TestCount_location extends FunSuite {
  Logger.getLogger("org").setLevel(Level.ERROR)
  var sparkSession : SparkSession  = SparkSession.builder()
    .master("local[*]")
    .appName("Jointable")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val lines = readfile(sparkSession.read.csv("src/main/resources/user.csv"),Seq ("userid", "mailid", "language", "country"))
  val lines1 = readfile(sparkSession.read.csv("src/main/resources/transactions.csv"),Seq ("transcationid", "productid", "userid", "price", "productdesc"))

  assert(Count_location.count(lines,lines1).toString=== lines.join (lines1, lines ("userid") === lines1("userid"), "inner")
    .groupBy("country").count().toString())

  sparkSession.stop()

}
