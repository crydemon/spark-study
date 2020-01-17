package kfk

import java.sql.{Connection, DriverManager}
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SQLContext}

//class MySQLSink(options: Map[String, String], outputMode: OutputMode) extends Sink {
//
//  override def addBatch(batchId: Long, data: DataFrame): Unit = {
//    /** 从option参数中获取需要的参数 **/
//    val userName = options.get("userName").orNull
//    val password = options.get("password").orNull
//    val table = options.get("table").orNull
//    val jdbcUrl = options.get("jdbcUrl").orNull
//    //data.show()
//    //println(userName+"---"+password+"---"+jdbcUrl)
//    val pro = new Properties
//    pro.setProperty("user", userName)
//    pro.setProperty("password", password)
//    data.write.mode(outputMode.toString).jdbc(jdbcUrl, table, pro)
//  }
//}
//
//class MySQLStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {
//
//  override def createSink(sqlContext: SQLContext,
//                          parameters: Map[String, String],
//                          partitionColumns: Seq[String],
//                          outputMode: OutputMode): Sink = {
//    new MySQLSink(parameters, outputMode);
//  }
//
//  //此名称可以在.format中使用。
//  override def shortName(): String = "mysql"
//}


/**
 * 自定义mysqk组件
 * @param url  输入database
 * @param userName
 * @param pwd
 */
class MysqlSink(url:String, userName:String, pwd:String) extends  ForeachWriter [Row]{
  //创建连接对象
  var conn:Connection = _
  //建立连接
  var i = 0
  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    conn=DriverManager.getConnection(url,userName,pwd)
    println(i + 1)
    i = i +1
    true
  }
  //数据写入mysql
  override def process(value: Row): Unit = {
    //注意streamingout1(k,v,t)是在 mysql 指定database中自己所创建的表，(k,v,t)分别是自己创建的键值对
    val p=conn.prepareStatement("insert into mysql_sink(id,val) values(?,?)")
    p.setString(1,value(0).toString)
    p.setString(2,value(1).toString)
    p.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}


object SparkConsumer extends App {
  // Subscribe to 1 topic
  val spark = utils.util.initSpark("kfk")

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "jj")
    .load()


  val read = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .toDF("id", "val")

  //创建流式数据写入格式对象   //连接到名为Spark的database
  val mysqlsink = new MysqlSink("jdbc:mysql://localhost:3306/test?serverTimezone=UTC","root","root")
  //将处理好得数据重新写入到mysql中去
  val query =  read.writeStream
    .outputMode(OutputMode.Append())//聚合操作的显示必须要使用complete,非聚合要使用append
    //检查点必须设置，不然会报错
    .option("checkpointLocation", "d://checkpoint//mysql")//设置检查点
    .foreach(mysqlsink)//输出到mysql
    .start()
  query.awaitTermination()


  spark.streams.awaitAnyTermination()
}

object SimpleKfa {

  def main(args: Array[String]): Unit = {
    product
    //consumer
  }

  def product = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "127.0.0.1:9092")
    prop.put("acks", "all")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("request.timeout.ms", "60000")
    val producer = new KafkaProducer[String, String](prop)
    // 模拟一些数据并发送给kafka
    for (i <- 1 to 100) {
      val msg = s"${i}: this is a jj ${i} kafka data"
      println("send -->" + msg)
      // 得到返回值
      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String]("jj", i.toString, msg)).get()
      println(rmd.toString)
    }

    producer.close()
  }

  def consumer = {
    // 配置信息
    val prop = new Properties
    prop.put("bootstrap.servers", "127.0.0.1:9092")
    // 指定消费者组
    prop.put("group.id", "group01")
    // 指定消费位置: earliest/latest/none
    prop.put("auto.offset.reset", "earliest")
    // 指定消费的key的反序列化方式
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列化方式
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("session.timeout.ms", "30000")
    // 得到Consumer实例
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    // 首先需要订阅topic
    kafkaConsumer.subscribe(Collections.singletonList("jj"))
    // 开始消费数据
    while (true) {
      // 如果Kafak中没有消息，会隔timeout这个值读一次。比如上面代码设置了2秒，也是就2秒后会查一次。
      // 如果Kafka中还有消息没有消费的话，会马上去读，而不需要等待。
      val msgs = kafkaConsumer.poll(2000)
      // println(msgs.count())
      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: ${msg.key()}, value: ${msg.value()}")
      }
    }
  }


}
