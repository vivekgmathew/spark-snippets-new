package dc.basics

object Test extends App {
  val s: Int => Int = x => x + 1
  println(List(1, 2, 3).map(s))
}
