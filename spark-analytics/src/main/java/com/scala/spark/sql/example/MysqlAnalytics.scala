package com.scala.spark.sql.example

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, ResultSet}

/**
* Example modified from Spark Training code
*/
object MysqlAnalytics {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: [sparkmaster]")
      exit(1)
    }
    val master = args(0)
    val sc = new SparkContext(master, "MysqlAnalytics", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM employee WHERE ? <= id AND ID <= ?",
      lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root");
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }
}
