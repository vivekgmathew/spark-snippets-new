package dc.df

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object ThreeCSV {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      appName("DataSources and Formats").
      config("spark.master", "local").
      getOrCreate()

    val stocksSchema = StructType (
      Array (
        StructField("symbol", StringType),
        StructField("date", DateType),
        StructField("price", DoubleType)
      )
    )

    val stockDf = spark.read
      .format("csv")
      .schema(stocksSchema)
      .option("dateFormat", "MMM dd YYYY")
      .option("header", true)
      .option("sep", ",")
      .option("nullValue", "")
      .load("src/main/resources/data/stocks.csv")

    // Parquet
    stockDf.write
      .mode(SaveMode.Overwrite)
      .parquet("folder_path")

    // Text files
    spark.read.text("file_path").show()

    // Reading From DB
    val dbDf = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/db_name")
      .option("user", "user_name")
      .option("password", "pass")
      .option("dbtable", "public.sales")
      .load()

    dbDf.show()

  }

}
