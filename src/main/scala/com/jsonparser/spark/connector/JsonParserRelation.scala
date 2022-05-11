package com.jsonparser.spark.connector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode_outer, from_json, size}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.mutable

/**
 * Implementation of Json Parser relation
 * @param config
 * @param schemaProvided
 * @param sparkSession
 */
class JsonParserRelation(
                        private  val config : Config,
                        schemaProvided: StructType
                      )
                        (@transient val sparkSession: SparkSession )
  extends BaseRelation with TableScan with Serializable {


  //Identifying the list of columns in input dataframe which needs parsing and flatten
  val jsonColumns = config("JsonColumns", "").toString.split(",").filter((a => !a.trim.isEmpty)).toSeq

  if (jsonColumns.length == 0)
    throw new Exception("Please specify 'JsonColumns' in options to parse the columns that contains json string.")

  //Identifying the list of json paths which shouldn't be expanded while flattening
  var dontExpandProperties = config("DonotExpand", "").toString.split(",").filter((a => !a.trim.isEmpty)).toSeq

  //Identifying the list of json paths which should be excluded while flattening
  var excludeProperties = config("Exclude", "").toString.split(",").filter((a => !a.trim.isEmpty)).toSeq



  //Container to hold the dataframe while flattening the input dataframe
  object Container {
    var dataFrame: org.apache.spark.sql.DataFrame = null
  }

  //Preparing dataframe based on properties sent to connector
  Container.dataFrame=getDataFrame

  //Returning the RDD of rows which should have the mentioned json columns flattened as rows and columns along with existing columns
  //in the given table
  val restResponseRDD: RDD[Row] = {

    val columnNameMap = mutable.HashMap[String, String]()


    //Foreach json columns mentioned in connector properties
    jsonColumns.foreach {

      jsonColumn => {

        var schema: StructType = null

        //connector property key to read the expected schema of the json string
        val schemaKey = jsonColumn + "_Schema"

        val ddlSchema: String = config(schemaKey, null)

        //Throwing error if schema for the mentioned json column is not sent to connector
        if (ddlSchema == null)
          throw new Exception(s"Please specify '$schemaKey' in options to parse the column : $jsonColumn that contains json string.")

        //Forming schema object based on mentioned DDL schema
        schema = StructType.fromDDL(ddlSchema.toString)


        //Checking if there is any alias for this json column to be considered while generating output columns
        val jsonColumnAlias = config(jsonColumn + "_Alias", jsonColumn)

        if (jsonColumnAlias != jsonColumn) {

          //If alias is mentioned then updating the expand and exclude properties to remove while parsing json

          dontExpandProperties = dontExpandProperties.map(a => (jsonColumnAlias) + (a.substring(jsonColumn.length)))

          excludeProperties = excludeProperties.map(a => (jsonColumnAlias) + (a.substring(jsonColumn.length)))

        }

        //Checking if the json column is a collection or object
        val isCollection = config(jsonColumn + "_IsCollection", "false").toBoolean

        //While flattening json columns, whether the output column names should have hierarchy or only the last property name
        val considerHierarchyInColumnNames = config(jsonColumn + "_ConsiderJsonHierarchyInOutputColumnNames", "true").toBoolean


        if (isCollection) {

          //If collection then exploding to multiple rows and then applying schema on json
          Container.dataFrame = Container.dataFrame.withColumn(jsonColumnAlias, explode_outer(from_json(col(jsonColumn), ArrayType(schema))))

        }
        else {

          //If not collection then just applying schema on the json
          Container.dataFrame = Container.dataFrame.withColumn(jsonColumnAlias, from_json(col(jsonColumn), schema))

        }

        //Dropping the actual json input column if alias is specified, since we no longer need it as we already applied schema and added as new column
        if (jsonColumnAlias != jsonColumn)
          Container.dataFrame = Container.dataFrame.drop(jsonColumn)

        //Flattening the dataframe on the specific json column
        flattenDataFrame(exclude = excludeProperties, dontExpand = dontExpandProperties, jsonColumnAlias, columnNameMap)

        //If to not consider hierarchy in output column names
        if (considerHierarchyInColumnNames == false) {

          //Finding duplicate json property names which may be in different hierarchy
          val duplicateColumns = columnNameMap.groupBy(c=>c._2).filter(c=>c._2.size>1)

          //Foreach json property column
          columnNameMap.foreach({

            kv => {

              val hirerachyColumnName = kv._1
              val columnName = kv._2

              if (Container.dataFrame.columns.contains(columnName)
                || duplicateColumns.contains(columnName))
              {

              }
              else {

                //If there is no duplicate json property name found in a different hierarchy, then considering it in the output dataframe instead of hierarchy name
                Container.dataFrame = Container.dataFrame.withColumnRenamed(hirerachyColumnName, columnName)

              }

            }

          })

        }

      }

    }

    Container.dataFrame.rdd

  }

  /**
   * Flattens the input dataframe on a given column and based on other connector properties.
   * This is a recursive function which navigates through the json hierarchy based on given schema
   * @param exclude
   * @param dontExpand
   * @param fieldName
   * @param columnNameMap
   */
  def flattenDataFrame(exclude: Seq[String],
                       dontExpand: Seq[String],
                       fieldName: String,
                       columnNameMap:mutable.HashMap[String,String]): Unit = {

    //Retrieving the field based on json column name
    val field = Container.dataFrame.schema.fields.filter(a => a.name == fieldName)(0)

    //Based on field data type
    field.dataType match {

       //If Json Property is object
      case structType: StructType => {

        //If it is asked to exclude this hierarchy, then dropping this hierarchy json from output dataframe
        if (exclude.contains(field.name)) {

          //Logging for troubleshooting purposes
          JsonLogger.trace("Excluding field :" + field.name)

          //Removing the json column hierarchy column from the dataframe
          Container.dataFrame = Container.dataFrame.drop(field.name)

        }
        else {

          //If not mentioned to not expand this json column hierarchy
          if (!dontExpand.contains(field.name)) {

            //Incase of object, navigating to each property in the hierarchy
            structType.fields.foreach {

              innerField => {

                //Adding the json property as a new column
                Container.dataFrame = Container.dataFrame.withColumn(field.name + "_" + innerField.name, col(field.name + ".`" + innerField.name+"`"))

                //Adding the column name hierarchy and the actual property to map
                columnNameMap +=  (field.name + "_" + innerField.name -> innerField.name)

                //Calling the same function to further navigate through the json hirerachy
                flattenDataFrame(exclude, dontExpand, field.name + "_" + innerField.name,columnNameMap)

              }

            }

            //Post expanding the current json column property, removing this specific json property column since it is already expanded to multiple columns
            Container.dataFrame = Container.dataFrame.drop(field.name)

          }
          else {

            //Logging for troubleshooting purposes
            JsonLogger.trace("Skipping Expand for field :" + field.name)

          }
        }

      }

      //If Json Property is collection
      case arrayType: ArrayType => {

        //Adding computed column which is the count of items in this json collection property.
        //This may help for getting idea of no. of items while reading this data without need to count the lines exploded as rows in this hierarchy
        Container.dataFrame = Container.dataFrame.withColumn(field.name + "_Count", size(col(field.name)))

        //If it is asked to exclude this hierarchy, then dropping this hierarchy json from output dataframe
        if (exclude.contains(field.name)) {

          //Logging for troubleshooting purposes
          JsonLogger.trace("Excluding field :" + field.name)

          //Removing the json column hierarchy column from the dataframe
          Container.dataFrame = Container.dataFrame.drop(field.name)

        }
        else {

          //If not mentioned to not expand this json column hierarchy
          if (!dontExpand.contains(field.name)) {

            //Exploding the json collection property as new rows
            //Post adding rows, dropping the json collection property
            Container.dataFrame = Container.dataFrame.withColumn(field.name + "_arr",
                                                                  explode_outer(col(field.name)))
                                                                  .drop(col(field.name))
                                                                  .withColumnRenamed(field.name + "_arr", field.name)

            //Calling the same function to further navigate through the json hirerachy
            flattenDataFrame(exclude, dontExpand, field.name, columnNameMap)

            //Dropping the column if the collection is set of objects
            if (field.dataType.asInstanceOf[ArrayType].elementType.typeName.toLowerCase.equals("struct"))
              Container.dataFrame = Container.dataFrame.drop(field.name)

          }
          else {

            //Logging for troubleshooting purposes
            JsonLogger.trace("Skipping Expand for field :" + field.name)

          }

        }
      }
      //Other than object or collection json property
      case _ => {
        if (exclude.contains(field.name)) {

          //Logging for troubleshooting purposes
          JsonLogger.trace("Excluding field :" + field.name)

          //Removing the json column hierarchy column from the dataframe
          Container.dataFrame = Container.dataFrame.drop(field.name)

        }
      }
    }
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  /**
   * Setting the  schema of the final dataframe that the json parser connector would return
   * @return
   */
  override def schema: StructType = Container.dataFrame.schema

  override def buildScan(): RDD[Row] = restResponseRDD


  /**
   * Return dataframe based on config parameters
   * @return
   */
  def getDataFrame:DataFrame={

    //Reading connector properties to create dataframe by
    //Location with Format or Table name
    val sourceLocation = config("Location", "").toString
    val sourceFormat = config("Format", "parquet").toString
    val sourceTable = config("Table", "").toString

    //Raising exception if both table and location are not sent to connector
    if (sourceTable == "" && sourceLocation == "")
      throw new Exception("Please specify 'Location' or 'Table' in options to read the data and invoking rest api.")

    var dataFrame: DataFrame = null

    //Constructing dataframe either by location or table
    if (sourceFormat != "" && sourceLocation != "" && sourceFormat != null && sourceLocation !=null) {
      dataFrame = sparkSession.read.format(sourceFormat).load(sourceLocation)
    }
    if (sourceTable != "" && sourceTable !=null) {
      dataFrame = sparkSession.sql(s"select * from $sourceTable")
    }
    dataFrame

  }

}
