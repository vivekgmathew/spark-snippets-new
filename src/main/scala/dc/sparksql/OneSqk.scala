package dc.sparksql

import org.apache.spark.sql.SparkSession

object OneSql extends App {
  val spark = SparkSession.builder().
    appName("Spark-SQL").
    config("spark.master", "local").
    getOrCreate()

  spark.sql("create database Test")
  spark.sql("use test")
  val dbDf = spark.sql("show databases")
  dbDf.show()

  // Reading from spark Data Warehouse Table
  val dwDF = spark.read.table("person")
  spark.sql(
    """
      |
    """.stripMargin)
}
