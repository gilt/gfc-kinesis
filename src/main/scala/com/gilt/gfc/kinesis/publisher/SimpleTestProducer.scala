package com.gilt.gfc.kinesis.producer

import com.gilt.gfc.kinesis.KinesisFactory
import com.gilt.gfc.kinesis.publisher.{PartitionKey, RawRecord, KinesisPublisherConfig}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

object SimpleTestStringProducer extends App {
  val config = new KinesisPublisherConfig {
    override val regionName = "us-east-1"
    //    override val regionName = "eu-west-1" // Ireland...
    override val maxConnectionCount = 50
  }

  val producer = KinesisFactory.newPublisher[String](
    "cclifford-simple-test-stream",
    config,
    s => RawRecord(s.getBytes, PartitionKey(s"pkey:$s"))
  )

  1.to(10).foreach { i =>
    println(s"Starting sending batch $i")
    val startMillis = System.currentTimeMillis
    val tries = Await.result(
      Future.sequence {
        1.to(5000).map { j =>
          producer.publish(s"This is event $j from batch $i")
        }
      }, 10.minutes)

    println(s"${tries.count(_.isSuccess)} sent, ${tries.count(_.isFailure)} failures, in ${System.currentTimeMillis - startMillis} millis")
    Thread.sleep(1000)
  }

  producer.shutdown()

  println("shutdown now..")
}
