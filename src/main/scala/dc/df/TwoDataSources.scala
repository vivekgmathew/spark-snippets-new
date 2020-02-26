package dc.df

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TwoDataSources {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      appName("DataSources and Formats").
      config("spark.master", "local").
      getOrCreate()

    val saleSchema = StructType(
      Array(
        StructField("Item", StringType),
        StructField("Description", StringType),
        StructField("Price", IntegerType)
      )
    )

    // Ex with options
    val salesDf = spark.read
      .format("json")
      .schema(saleSchema)
      .option("mode", "failFast") // dropMalformed, permissive (default)
      .load("file_path")

    // Ex with options map
    val salesOptionMap = spark.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "inferSchema" -> "true",
        "path" -> "file_path"
      ))
      .load()

    // Writing Data-frames
    salesDf.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("file_path")

    // JSON flags
    spark.read
      .schema(saleSchema)
      .option("dateFormat", "YYYY-MM-DD")
      .json("file_path")


  }

}
