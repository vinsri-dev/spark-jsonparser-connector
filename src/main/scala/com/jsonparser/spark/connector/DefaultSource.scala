package com.jsonparser.spark.connector

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Entry point for Json Parser Connector
 */

class DefaultSource extends DataSourceRegister with SchemaRelationProvider {

  //connector can be called using below short name
  override def shortName(): String = "jsonparser"

  /**
   * Override implementation to return appropriate relation which does parse json columns based
   * on configuration settings and data in rdd rows
   * @param sqlContext
   * @param parameters
   * @param schema
   * @return
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): JsonParserRelation = {

    new JsonParserRelation(getConfig(sqlContext, parameters), schema)(sqlContext.sparkSession)

  }

  /**
   * Returns config object based on key value pair sent while calling connector
   * Config properties are also read from configuration file if specified in the key value pair with appropriate path
   * @param sqlContext
   * @param parameters
   * @return
   */
  def getConfig(sqlContext: SQLContext,
                parameters: Map[String, String]): Config = {

    //Prepring config object
    var config = Config(sqlContext.sparkSession, parameters)

    config = Config(sqlContext.sparkSession,  parameters)

    config
  }

}
