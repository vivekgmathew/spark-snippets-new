package dc.ds

import org.apache.spark.sql.SparkSession

object OneCommonTypes extends App {
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

  moviesDf.show()
}
