package com.jsonparser.spark.connector

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import play.api.libs.json.{JsObject, Json}

import java.util.Random
import scala.io.Source
class BaseTest extends AnyFlatSpec {

  var rand = new Random()

  def getRandomString: String = ((rand.nextInt(10000)) + "_" + (rand.nextInt(10000)) + "_" + (rand.nextInt(10000))).toUpperCase()

  def printDataFrame(df: DataFrame): Unit = {
    println(df.show())
  }

  def parseFirstRowAndFirstColumnAsJSON(df: DataFrame): scala.collection.Map[String, String] = {
    val jsonObj = Json.parse(df.first().getString(0))
    jsonObj.asInstanceOf[JsObject].value
      .map(
        (a)=>{
          (a._1,a._2.toString())
        }
      )
  }

}
