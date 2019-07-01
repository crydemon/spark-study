class FutureTest extends App {

  import scala.concurrent.Future
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.blocking
  import scala.concurrent.duration._

  def f(item: Int): Future[Unit] = Future {
    print("Waiting " + item + " seconds ...")
    Console.flush
    blocking {
      Thread.sleep((item seconds).toMillis)
    }
    println("Done")
  }

  val fSerial = f(4) flatMap (res1 => f(16)) flatMap (res2 => f(2)) flatMap (res3 => f(8))

  fSerial.onComplete { case resTry => println("!!!! That's a wrap !!!! Success=" + resTry.isSuccess) }


}
