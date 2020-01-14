package utils

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.sys.process._

object KafkaUtil {
  val kafkaHome = "D:\\tools\\kafka_2.12-2.4.0\\"

  def main(args: Array[String]): Unit = {
    "start" match {
      case "start" => startKfk()
      case _ => stopKfk()
    }
  }

  def startKfk() = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val f1 = Future {
      s"${kafkaHome}bin\\windows\\zookeeper-server-start.bat ${kafkaHome}config\\zookeeper.properties ".!
    }
    val f2 = Future {
      s"${kafkaHome}bin\\windows\\kafka-server-start.bat  ${kafkaHome}config\\server.properties ".!
    }
    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)
  }

  def stopKfk() = {
    s"${kafkaHome}bin\\windows\\kafka-server-stop.bat".!
    s"${kafkaHome}bin\\windows\\zookeeper-server-stop.bat".!
  }
}
