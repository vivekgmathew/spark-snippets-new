package dc.df

import org.apache.spark.sql.SparkSession

object FiveColExpPractise extends App {

  val spark = SparkSession.builder().
    appName("Columns and Expressions Practise").
    config("spark.master", "local").
    getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val movieDf = spark.read.option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // Select good comedy movie.
  val goodComedy = movieDf.select("Title", "IMDB_Rating")
    .where($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)

  goodComedy.show()

}
