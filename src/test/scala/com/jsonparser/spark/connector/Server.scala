package com.jsonparser.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Server {

  def sparkConf:SparkConf={
    var sparkConf=new SparkConf()
      .set("test","1")
    sparkConf
  }
  def sparkSession:SparkSession={
    var _spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("test-sql-context1")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    _spark
  }
}
