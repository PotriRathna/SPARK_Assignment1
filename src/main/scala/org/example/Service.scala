package org.example
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

object Service {

  def count(userdf:DataFrame,transdf:DataFrame):DataFrame = userdf.join (transdf, userdf ("userid") === transdf ("userid"), "inner")
    .groupBy("country").count().withColumnRenamed("country","Location")

  // b).  Find out products bought by each user.
  def product_bought(transdf:DataFrame):DataFrame = transdf.select("productid", "userid")
    .groupBy("userid").count().withColumnRenamed("userid","User")

  // c)	Total spending done by each user on each product.
  def Spending_eachuser(transdf:DataFrame):DataFrame= transdf.groupBy("userid","productid")
    .agg(sum("price")).withColumnRenamed("userid","User")

  def readfile(file:DataFrame,col:Seq[String]):DataFrame= file.toDF(col:_*)

}
