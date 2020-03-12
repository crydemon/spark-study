package spark

object DirtyWork extends App {

  0.to(10).map(x => {
    println(x + ","); x
  }).map(x => {
    println(2 * x + ","); 2 * x
  })
}
