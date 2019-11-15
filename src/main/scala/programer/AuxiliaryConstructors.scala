package programer
//
//
//There are several important points to this recipe:
//
//Auxiliary constructors are defined by creating methods named this.
//
//Each auxiliary constructor must begin with a call to a previously defined constructor.
//
//Each constructor must have a different signature.
//
//One constructor calls another constructor with the name this.

object AuxiliaryConstructors extends App {
  val p1 = new Pizza(12, "gg")
  val p2 = new Pizza(Pizza.DEFAULT_CRUST_SIZE)
  val p3 = new Pizza(Pizza.DEFAULT_CRUST_TYPE)
  val p4 = new Pizza
  println(p1)
  println(p2)
  println(p3)
  println(p4)
}

// primary constructor
class Pizza (var crustSize: Int, var crustType: String) {

  // one-arg auxiliary constructor
  def this(crustSize: Int) {
    this(crustSize, Pizza.DEFAULT_CRUST_TYPE)
  }

  // one-arg auxiliary constructor
  def this(crustType: String) {
    this(Pizza.DEFAULT_CRUST_SIZE, crustType)
  }

  // zero-arg auxiliary constructor
  def this() {
    this(Pizza.DEFAULT_CRUST_SIZE, Pizza.DEFAULT_CRUST_TYPE)
  }

  override def toString = s"A $crustSize inch pizza with a $crustType crust"

}

object Pizza {
  val DEFAULT_CRUST_SIZE = 12
  val DEFAULT_CRUST_TYPE = "THIN"
}


// the case class
case class Person (var name: String, var age: Int)

// the companion object
object Person {

  def apply() = new Person("<no name>", 0)
  def apply(name: String) = new Person(name, 0)

}

object CaseClassTest extends App {

  val a = Person()         // corresponds to apply()
  val b = Person("Pam")    // corresponds to apply(name: String)
  val c = Person("William Shatner", 82)

  println(a)
  println(b)
  println(c)

  // verify the setter methods work
  a.name = "Leonard Nimoy"
  a.age = 82
  println(a)
}