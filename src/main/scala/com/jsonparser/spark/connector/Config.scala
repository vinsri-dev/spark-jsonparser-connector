package com.jsonparser.spark.connector

import Config.Property
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

import scala.reflect.ClassTag

/**
 * Configuration properties interface
 */
trait Config extends  Serializable {

  var properties: Map[Property, Any]

  /**
   * Return configuration value. If no property exists with config key, return default value
   * @param property
   * @param default
   * @tparam T
   * @return
   */
  def apply[T: ClassTag](property: Property,
                         default : =>T): T = {
    val value = get[T](property)
    if (value == None)
      return default
    else value.get
  }

  /**
   * Return configuration value. If no property exists with config key, raise exception
   * @param property
   * @tparam T
   * @return
   */
  def apply[T: ClassTag](property: Property): T = {
    val value = get[T](property)
    if (value == None)
      throw new Exception(s"Configuration property : $property missing.")
    value.get
  }

  /**
   * Get configuration property value by key
   * @param property
   * @tparam T
   * @return
   */
  def get[T: ClassTag](property: Property): Option[T] =
    properties.get(property.toLowerCase()).map(_.asInstanceOf[T])

  /**
   * Set configuration property value by key and given value
   * @param property
   * @param value
   * @tparam T
   */
  def set[T: ClassTag](property: Property,
                       value:String):Unit =
    properties =  properties ++ Map[Property,String](property.toLowerCase()->value)

  /**
   * Return configuration key, values as Map object
   * @return
   */
  def asOptions: collection.Map[String, String] = {
    properties.map { case (x, v) => x.toLowerCase() -> v.toString() }
  }
}

/**
 * Configuration object
 */
object Config {

  type Property = String

  /**
   * Return Config object with values from given input Map object
   * @param spark
   * @param options
   * @return
   */
  def apply(spark: SparkSession,
            options: collection.Map[String, String]): Config = new Config {

    //Check if config file is provided
    // Incase of sensitive information like passwords, access keys, connector can read from config file
    val configFilePath = options.getOrElse("configfile", "")

    var configFileParams = Map[String, String]()

    //If config file is provided, then reading the properties from file
    if (configFilePath != "" && spark!=null) {

      try {

        //Reading config file
        val configJson = spark.sparkContext.textFile(configFilePath).collect().mkString("")

        //Parsing config file
        val configJsonDef = Json.parse(configJson)
        val configParameters = configJsonDef.as[List[KeyValuePair]]

        //Adding config parameters to config map
        configParameters.foreach(p => configFileParams += (p.key -> p.value))
      }
      catch {
        case e: InvalidInputException => {
          throw new Exception(s"Config parameter -> 'configfile' is specified with a file that doesn't exist. Configuration Parameter Value : $configFilePath . Please check the 'configfile' parameter", e)
        }
      }

    }

    /**
     * Retruning properties upon appending properties from config file and properties sent by connector explicitly
     */
    override var properties: Map[Property, Any] = (configFileParams ++ options).map { case (x, v) => x.toLowerCase() -> v.toString() }.asInstanceOf[Map[Property, Any]]
  }
}