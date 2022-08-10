package com.jsonparser.spark.connector

import play.api.libs.json.{JsObject, Json}

class BasicJsonTests extends BaseTest {

  val student1Json = """{
                      |"Name":"Name1",
                      |"Age":10,
                      |"Subjects":[{"English":80, "Maths":90, "Social":80}]
                      |}
                      |""".stripMargin

  val student2Json = """{
                      |"Name":"Name2",
                      |"Age":11,
                      |"Subjects":[{"English":60, "Maths":95, "Social":70}]
                      |}
                      |""".stripMargin


  /**
   * Basic Json happy scenario
   */
  "Basic : Calling JsonParsers Default Source with Simple Json Strings" should " pass" in {

    val spark2 = Server.sparkSession
    import spark2.implicits._


    //Static dataframe with 2 rows.
    //Each row has student specific score card as json object

    var inputDF = Seq(
      ("Name1 Student Score Card", student1Json),
      ("Name2 Student Score Card", student2Json)
    ).toDF("Notes", "Score")

    inputDF.createOrReplaceTempView("Students")

    val vwName1 = getRandomString

    //Sql using the json parser connector and passing appropriate properties
    val tempView =
      s"""
         CREATE Or replace TEMPORARY VIEW DrugsDosage
         |USING com.jsonparser.spark.connector
         |OPTIONS (
         |Table "Dosage",
         |JsonColumns "Drug",
         |Drug_Schema "`problems` ARRAY<STRUCT<`Diabetes`: ARRAY<STRUCT<`labs`: ARRAY<STRUCT<`missing_field`: STRING>>, `medications`: ARRAY<STRUCT<`medicationsClasses`: ARRAY<STRUCT<`className`: ARRAY<STRUCT<`associatedDrug`: ARRAY<STRUCT<`dose`: STRING, `name`: STRING, `strength`: STRING>>>>, `className2`: ARRAY<STRUCT<`associatedDrug`: ARRAY<STRUCT<`dose`: STRING, `name`: STRING, `strength`: STRING>>>>>>>>>>>>"
         |)""".stripMargin
    Server.sparkSession.sql(tempView)


    //Filtering for specific row
    val drugName = Server.sparkSession.sql(
      s"select Drug_problems_Diabetes_medications_medicationsClasses_className_associatedDrug_name from DrugsDosage " +
        s"where Drug_problems_Diabetes_medications_medicationsClasses_className2_associatedDrug_name='asprin2' " +
        s"order by Drug_problems_Diabetes_medications_medicationsClasses_className_associatedDrug_name").first().getString(0)


    assert(drugName.contentEquals("asprin"))

  }
}