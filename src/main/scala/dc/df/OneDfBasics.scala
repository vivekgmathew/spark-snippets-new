package dc.df

import org.apache.spark.sql.SparkSession

object OneDfBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
        appName("DataFrame Basics").
        config("spark.master", "local").
        getOrCreate()

    val firstDf = spark.read.
      format("json").
      option("inferSchema", "true").
      load("src/main/resources/data/cars.json")

    firstDf.show()

    import spark.implicits._

    val smartPhones = Seq(
      ("Samsung", "Galaxy S10", "Android", 10),
      ("Apple", "Iphone X", "IOS", 12)
    )

    val smartPhoneDf = smartPhones.toDF("Make", "Model", "Platform", "Camera")
    smartPhoneDf.show()

  }
}
