package com.gilt.gfc.kinesis.publisher

import java.util.concurrent.atomic.AtomicInteger

import com.gilt.gfc.kinesis.KinesisFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

object SimpleTestStringProducer extends App {

  val config = new KinesisPublisherConfig {
    override val regionName = "us-east-1"
    override val maxConnectionCount = 200
  }

  def convert(str: String) = RawRecord(str.getBytes, PartitionKey("somepartitionkey"))

  val publisher = KinesisFactory.newPublisher(
    "cclifford-gfc-kin-testing",
    config,
    convert
  )

  println("Starting...")

  val failures = new AtomicInteger(0)
  @volatile var i = 0

  0.to(10).foreach { _ =>
    println("starting publishing batch...")
    val batchStartTime = System.currentTimeMillis

    Await.ready(Future.sequence {
      0.to(1000).map { _ =>
        i += 1
        publisher.publish(s"Record $i").recover { case ex => Failure(ex)}.map {
          case Failure(_) => failures.incrementAndGet()
          case _ =>
        }
      }
    }, Duration.Inf)

    println(s"Batch completely published in ${System.currentTimeMillis - batchStartTime}, failures = ${failures.getAndSet(0)}")
    Thread.sleep(1000)
  }
  println("Done done done.")
}
