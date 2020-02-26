package dc.df

import org.apache.spark.sql.SparkSession

object SevenJoins extends App {

  val spark = SparkSession.builder().
    appName("Joins").
    config("spark.master", "local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val guitarDf = spark.read.option("inferSchema", true)
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDf = spark.read.option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDf = spark.read.option("inferSchema", true)
    .json("src/main/resources/data/bands.json")

  // Inner join
  val guitaritsBandDf = guitarPlayersDf.join(bandsDf,
    guitarPlayersDf.col("band") === bandsDf.col("id"), "inner")
  guitaritsBandDf.show()


}
