package com.jsonparser.spark.connector

class ComplexJsonTests extends BaseTest {

  val complexJson1 = """{
"problems": [{
    "Diabetes":[{
        "medications":[{
            "medicationsClasses":[{
                "className":[{
                    "associatedDrug":[{
                        "name":"asprin",
                        "dose":"",
                        "strength":"500 mg"
                    },
                    {
                        "name":"somethingElse",
                        "dose":"",
                        "strength":"500 mg"
                    }]
                }],
                "className2":[{
                    "associatedDrug":[{
                        "name":"asprin2",
                        "dose":"",
                        "strength":"500 mg"
                    },{
                        "name":"somethingElse",
                        "dose":"",
                        "strength":"500 mg"
                    }
                    ]
                }]
            }]
        }],
        "labs":[{
            "missing_field": "missing_value"
        }]
    }],
    "Asthma":[{}]
}]}""".stripMargin




  /**
   * Complex Json happy scenario
   */
  "Complex : Calling JsonParsers Default Source with Complex Json Strings" should " pass" in {

    val spark2 = Server.sparkSession
    import spark2.implicits._


    //Static dataframe with 1 row

    var inputDF=Seq(
      ("Complex Json",complexJson1)
    ).toDF("Notes","Drug")
    inputDF.createOrReplaceTempView("Dosage")


    val vwName1 = getRandomString

    //Sql using the json parser connector and passing appropriate properties
    val tempView =
      s"""
         CREATE Or replace TEMPORARY VIEW DrugsDosage
         |(
         |  Notes string,
         |  Drug string
         |)
         |USING com.jsonparser.spark.connector
         |OPTIONS (
         |Table "Dosage",
         |JsonColumns "Drug",
         |Drug_Schema "`problems` ARRAY<STRUCT<`Diabetes`: ARRAY<STRUCT<`labs`: ARRAY<STRUCT<`missing_field`: STRING>>, `medications`: ARRAY<STRUCT<`medicationsClasses`: ARRAY<STRUCT<`className`: ARRAY<STRUCT<`associatedDrug`: ARRAY<STRUCT<`dose`: STRING, `name`: STRING, `strength`: STRING>>>>, `className2`: ARRAY<STRUCT<`associatedDrug`: ARRAY<STRUCT<`dose`: STRING, `name`: STRING, `strength`: STRING>>>>>>>>>>>>"
         |))""".stripMargin
    Server.sparkSession.sql(tempView)


    //Filtering for student 1
    val student1EnglishScore = Server.sparkSession.sql(
      s"select English from StudentsScoreSheet where Name = 'Name1'").first().getLong(0)

    //Filtering for student 1
    val student2MathsScore = Server.sparkSession.sql(
      s"select Maths from StudentsScoreSheet where Name = 'Name2'").first().getLong(0)

    assert(student1EnglishScore == 80)
    assert(student2MathsScore == 95)

  }
}
