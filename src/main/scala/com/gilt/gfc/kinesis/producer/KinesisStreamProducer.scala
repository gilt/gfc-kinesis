package com.gilt.gfc.kinesis.producer

import com.gilt.gfc.kinesis.producer.raw.{RawRecord, RawKinesisStreamProducer}
import com.gilt.gfc.logging.Loggable

import scala.util.{Failure, Success, Try}


case class KinesisStreamProducer[T](streamName: String,
                                    config: KinesisProducerConfig,
                                    convert: T => RawRecord) extends Loggable {

  private val rawProducer = RawKinesisStreamProducer(streamName, config)

  @volatile private var errorHandler = defaultErrorHandler _


  def onError(handler: Exception => Unit): Unit = {
    errorHandler = handler
  }

  def put(event: T): Unit = {
    Try(convert(event)) match {
      case Success(record) => rawProducer.putRecord(record)
      case Failure(ex: Exception) => errorHandler(ex)
      case Failure(t) => throw t // Propogate unhandled (should not happen...)
    }
  }

  private def defaultErrorHandler(ex: Exception): Unit = {
    error("Failed to convert event using application supplied function", ex)
  }

}
