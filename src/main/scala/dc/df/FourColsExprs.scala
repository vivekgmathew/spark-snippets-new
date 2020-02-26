package dc.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FourColsExprs {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      appName("Columns and Expressions").
      config("spark.master", "local").
      getOrCreate()

    val carsDf = spark.read
      .option("inferSchema", true)
      .json("src/main/resources/data/cars.json")

    carsDf.show()

    // Columns
    val firstCol = carsDf.col("Name")

    // Selection
    val carNamesDf = carsDf.select(firstCol)
    carNamesDf.show()

    // Various Selection Options
    import spark.implicits._
    val multiColDf = carsDf.select(
      carsDf.col("Name"),
      col("Acceleration"),
      column("Weight_in_lbs"),
      'Year,
      $"Horsepower",
      exp("Origin")
    )

    // Filtering
    val euCars = carsDf.filter(col("Origin") =!= "USA")
    val euCars2 = carsDf.where(col("Origin") =!= "USA")

    // Filtering with expression strings
    val usaCars = carsDf.where("Origin = 'USA'")

    // MultipleFilters options
    // a. Chaining
    val usaPowCars = carsDf.where("Origin = 'USA'").filter("HorsePower > 150")

    // b. and method
    val usaPowerCars2 = carsDf.filter(col("Origin") === "USA" and col("HorsePower") > 150)

    // c. expression
    val usaPowerCards = carsDf.filter("Origin = 'USA and HorsePower > 150")

  }

}
