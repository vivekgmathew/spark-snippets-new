package dc.ds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TwoComplexTypes extends App {

  val spark = SparkSession.builder().
    appName("Columns and Expressions Practise").
    config("spark.master", "local").
    getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDf = spark.read.
    format("json").
    option("inferSchema", "true").
    load("src/main/resources/data/movies.json")

  // Dates
  moviesDf.select($"Title", to_date($"Release_Date", "dd-MMM-yy").as("Actual Date")).show()


}
