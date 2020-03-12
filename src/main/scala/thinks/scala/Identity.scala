package thinks.scala

object Identity extends App {
  val arr = Array(234, 423, 423, 432, 0, 3, 5, 2, 2, 2, 1, 1, 1, 1)
  println(arr.groupBy(identity).values.map(_.size))
}


