package dc.df


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SixAggregations extends App {

  val spark = SparkSession.builder().
    appName("Aggregations and Grouping").
    config("spark.master", "local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  val movieDf = spark.read.option("inferSchema", true)
    .json("src/main/resources/data/movies.json")


  // Counting
  val genresCount = movieDf.select(count($"Major_Genre").as("Count"))
  genresCount.show()
  movieDf.select(countDistinct($"Major_Genre").as("Distinct Genres")).show()

  // Grouping
  movieDf.groupBy($"Major_Genre")
    .count().as("Count").show()

  movieDf.groupBy($"Major_Genre")
    .agg(count("*").as("Count"),
      avg($"IMDB_Rating").as("Avg IMDB")).show()

}
