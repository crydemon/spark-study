package thinks

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object ThinkImplicit1 extends App {
  val f1 = Future {
    Thread.sleep(100)
    println("test implicit parameters")
  }
  f1.onComplete {
    case Success(_) => println("success")
    case Failure(_) => println("false")
  }
  Await.result(f1, Duration.Inf)
}

object ThinkImplicit2 extends App {
  def person(implicit name: String) = name

  //implicit val name = "peter"
  implicit val other = "john"
  println(person)
}


object ThinkImplicit3 extends App {

  def person(implicit name: String) = name

  implicit def int2string(x: Int) = x.toString

  //implicit val other = 3 必须要同类型哦 隐式操作不能嵌套使用
  println(person(3))
}

//此时编译器就会在作用域范围内查找能使其编译通过的隐式视图，找到learningType方法后，
//编译器通过隐式转换将对象转换成具有这个方法的对象，之后调用wantLearning方法
//可以将隐式转换函数定义在伴生对象中，在使用时导入隐式视图到作用域中即可
class SwingType {
  def wantLearned(sw: String) = println("兔子已经学会了" + sw)
}

object swimming {
  implicit def learningType(s: AnimalType) = new SwingType
}

class AnimalType

object ThinkImplicit4 extends App {

  import swimming._

  val rabbit = new AnimalType
  rabbit.wantLearned("breaststroke") //蛙泳
}

// implicit def  originalToTarget (<argument> : OriginalType) : TargetType




