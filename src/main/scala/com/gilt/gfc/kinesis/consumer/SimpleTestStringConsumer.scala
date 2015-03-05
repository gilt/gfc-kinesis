package com.gilt.gfc.kinesis.consumer

import com.gilt.gfc.kinesis.KinesisFactory
import com.gilt.gfc.kinesis.publisher.RawRecord

object SimpleTestStringConsumer extends App {

  val config = new KinesisConsumerConfig {
    override def appName: String = "cclifford-simple-test-consumer"
  }

  def converter(record: RawRecord): Option[String] = {
    println(s"Converting RawRecord(${record.data}, ${record.partitionKey}")
    Some(new String(record.data))
  }

  val receiver = KinesisFactory.newReceiver[String]("cclifford-simple-test-stream", config, converter)
  println("receiver created")

  receiver.registerConsumer { str => println(s"consuming: $str") }
  println("consumer registered")

  receiver.start()
  println("receiver started")
}
