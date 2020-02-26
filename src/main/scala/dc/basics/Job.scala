package dc.basics

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object Job extends App {

  val spark = SparkSession.builder().
    appName("CapitalOne-COBOL").
    master("local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val cpudSchema = StructType (
     Array(
       StructField("CPUD_ORG", StringType),
       StructField("CPUD_POS", StringType),
       StructField("CPUD_ITEMS", ArrayType (
         StructType(Array(
           StructField("CPUD_ITEM_DESC", StringType),
           StructField("CPUD_QUANTITY", StringType))
         )
       ))
     )
  )

  val cpudData = Seq(
    Row("CapOne", "Walmart", List(Row("Pen", "2"), Row("Book", "3"))),
    Row("CapOne", "Target", List(Row("Bed", "1"), Row("Pillow", "2")))
  )

  val cpDf = spark.createDataFrame(spark.sparkContext.parallelize(cpudData), cpudSchema)
  println("##########################")
  println("ORIGINAL DF")
  println("##########################")
  cpDf.show()

  // Actual logic to explode and then flatten the array of struct values
  // You can use the same for your data.
  val explodedDf = cpDf.select($"CPUD_ORG", $"CPUD_POS", explode($"CPUD_ITEMS").as("CPUD_ITEMS"))
  val flattenedDf = explodedDf.select($"CPUD_ORG", $"CPUD_POS", $"CPUD_ITEMS.*")

  println("##########################")
  println("FINAL RESULT DF")
  println("##########################")
  flattenedDf.show()

}
