package algo

import scala.util.Random

class SkipListNode[T >: Null <: AnyRef](k: Int, v: T) {
  var Seq(up, down, left, right) = 1.to(4).map(_ => null: SkipListNode[T])

  def getKey = k

  def getValue = v

  override def toString: String =
    "key-value:" + k + "-" + v
}

object SkipListNode {
  val HEAD_KEY = Int.MaxValue
  val TAIL_KEY = Int.MinValue
}

class SkipList[T >: Null <: AnyRef] {
  var Seq(head, tail) = 1.to(2).map(_ => null: SkipListNode[T])
  var nodes = 0
  var ListLevel = 0
  val PROBABILITY = 0.5

  def clear = {
    head = new SkipListNode(SkipListNode.HEAD_KEY, null: T)


  }


}