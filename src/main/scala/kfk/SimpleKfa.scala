package kfk

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


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
    .as[(String, String)]

  val query = read.writeStream.format("console").start()

  Thread.sleep(1000000000)
  query.stop()
}

object SimpleKfa {

  def main(args: Array[String]): Unit = {
    product
    //consumer
  }

  def product={
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
  def consumer ={
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
