package com.vova.export

object Closures {
  
  def main(args: Array[String]): Unit = {
    val addOne = makeAdd(1)
    val addTwo = makeAdd(2)
    
    println(addOne(1))
    println(addTwo(1))
  }
  
  def makeAdd(more: Int) = (x: Int) => x + more
  
  def normalAdd(a: Int, b: Int) = a + b
  
}