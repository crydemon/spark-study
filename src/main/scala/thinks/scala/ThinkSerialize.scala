import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}



object FileSerializer {

  def writeObjectToFile(obj: Object, file: String) = {
    val fileStream = new FileOutputStream(file)
    val oos = new ObjectOutputStream(fileStream)
    oos.writeObject(obj)
    oos.close()
  }

  def readObjectFromFile(file: String): Object = {
    val fileStream = new FileInputStream(file)
    val ois = new ObjectInputStream(fileStream)
    val obj = ois.readObject()
    ois.close()
    obj
  }

  def main(args: Array[String]): Unit = {

  }
}
object WriteO extends App{
  import thinks.scala.{SimpleTask, Task}
  val task = new SimpleTask()
  FileSerializer.writeObjectToFile(task, "task.ser")
}

object ReadO extends App{
  import thinks.scala.Task
  val task = FileSerializer.readObjectFromFile("task.ser").asInstanceOf[Task]
  task.run()
}