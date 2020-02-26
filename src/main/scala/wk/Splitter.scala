package wk

import scala.io.Source

object Splitter {

  def main(args: Array[String]): Unit = {
    val jsonFile = args(0)
    val js = ujson.read(Source.fromFile(jsonFile).mkString)

    for((k, v) <- js.obj) {
      val key = k
      val value = v
      val num = 1
    }

  }

}
