package ia.one_basics

import org.apache.spark.sql.SparkSession

object DB extends App {

  val spark = SparkSession.builder()
    .appName("DB Load")
    .master("local")
    .getOrCreate()

  val baseDf = spark.read
    .format("csv")
    .option("header", true)
    .load()
}
