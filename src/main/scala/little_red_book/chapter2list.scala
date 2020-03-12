//package little_red_book
//
//
//
//case object Nil extends List[Nothing]
//
//case class Cons[+A](head: A, tail: List[A]) extends List[A]
//
//object List {
//
//  def apply[A](as: A*): List[A] =
//    if (as.isEmpty) Nil else Cons(as.head, apply(as.tail: _*))
//
//  def sum(l: List[Int]) = {
//    def go(acc: Int, ll: List[Int]): Int = ll match {
//      case Nil => acc
//      case Cons(h, t) => go(acc + h, t)
//    }
//
//    go(0, l)
//  }
//
//  def tail[A](l: List[A]) = l match {
//    case Nil => Nil
//    case Cons(_, t) => t
//  }
//
//  def setHead[A](l: List[A], h: A) = l match {
//    case Nil => Cons(h, Nil)
//    case Cons(_, t) => Cons(h, t)
//  }
//
//  def drop[A](l: List[A], n: Int): List[A] = l match {
//    case Nil => Nil
//    case Cons(_, t) if (n == 0) => t
//    case Cons(_, t) => drop(t, n - 1)
//  }
//
//  //f不能利用类型推导,我们常通过将参数拆分，最大化类型推导
//  def dropWhile[A](l: List[A], f: A => Boolean): List[A] = {
//    l match {
//      case Cons(h, t) if f(h) => dropWhile(t, f)
//      case _ => l
//    }
//  }
//
//
//  //非栈安全
//  def foldRight[A, B](l: List[A], z: B)(f: (A, B) => B): B = {
//    l match {
//      case Nil => z
//      case Cons(h, t) => f(h, foldRight(t, z)(f))
//    }
//  }
//
//  def foldLeft[A, B](l: List[A], z: B)(f: (B, A) => B): B = {
//    l match {
//      case Nil => z
//      case Cons(h, t) => foldLeft(t, f(z, h))(f)
//    }
//  }
//
//  def foldRight1[A, B](l: List[A], z: B)(f: (A, B) => B): B = {
//    foldLeft(reverse(l), z)((x, y) => f(y, x))
//  }
//
//  def reverse[A](l: List[A]): List[A] = foldLeft(l, Nil: List[A])((x, y) => Cons(y, x))
//
//  def length[A](l: List[A]): Int = foldRight(l, 0)((a, z) => z + 1)
//
//  //
//  //f可以利用类型推导
//  def dropWhile1[A](l: List[A])(f: A => Boolean): List[A] = {
//    l match {
//      case Cons(h, t) if f(h) => dropWhile1(t)(f)
//      case _ => l
//    }
//  }
//
//  def append[A](a1: List[A], a2: List[A]): List[A] = a1 match {
//    case Nil => a2
//    case Cons(h, t) => Cons(h, append(t, a2))
//  }
//
//  def append1[A](a1: List[A], a2: List[A]): List[A] = {
//    foldRight(a1, a2)((x, y) => Cons(x, y))
//  }
//
//  def concat[A](l: List[List[A]]): List[A] = {
//    foldRight(l, Nil: List[A])(append1)
//  }
//
//  def doubleToString(l: List[Double]): List[String] =
//    foldRight(l, Nil: List[String])((h, t) => Cons(h.toString, t))
//
//  def map[A, B](l: List[A])(f: A => B): List[B] =
//    foldRight(l, Nil: List[B])((h, t) => Cons(f(h), t))
//
//  def filter[A](l: List[A])(f: A => Boolean): List[A] =
//    foldRight(l, Nil: List[A])((h, t) => if (f(h)) Cons(h, t) else t)
//
//
//  def flatMap[A, B](l: List[A])(f: A => List[B]): List[B] = {
//    concat(map(l)(f))
//  }
//
//  def filter1[A](l: List[A])(f: A => Boolean): List[A] =
//    flatMap(l)((a) => if (f(a)) List(a) else Nil)
//
//  def toString[A](l: List[A]): String = {
//    foldRight(l, "")((x, y) => x + " " + y)
//  }
//
//}
//
//object Test {
//
//  def main(args: Array[String]): Unit = {
//
//    List.dropWhile(List(1, 2, 3, 4), (x: Int) => x < 2)
//    List.dropWhile1(List(1, 2, 3, 4))(x => x < 2)
//    println(List.length(List(23, 423423, 3423, 4234, 3432)))
//    val l = List(1, 2, 3, 4)
//    val l1 = List(5, 6, 7)
//    println(List.reverse(l))
//    println(List.append1(l, l1))
//  }
//}