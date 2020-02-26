package rc.onebasics

import org.apache.spark.{SparkConf, SparkContext}

object OneTest {
  def main(args: Array[String]): Unit = {

    val lst = Vector(1, 3, 4)

    val sconf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[*]")

    val sc = new SparkContext(sconf)
    println((sc.parallelize(lst).count()))

  }
}
