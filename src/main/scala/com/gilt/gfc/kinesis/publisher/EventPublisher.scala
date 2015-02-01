package com.gilt.gfc.kinesis.publisher

import com.gilt.gfc.kinesis.publisher.KinesisPublisherConfig

import scala.concurrent.Future
import scala.util.Try

/**
 * Kinesis Publisher of events of a certain type.
 *
 * See [[KinesisFactory.newPublisher()]]
 *
 * @tparam T
 */
trait EventPublisher[T] {
  /**
   * publish an event to the stream.
   *
   * This function requests that the event be published, which will be done asynchronously in the future.
   *
   * @param event
   * @return
   */
  def publish(event: T): Future[Try[Unit]]

  def shutdown(): Unit
}

private[kinesis] class EventPublisherImpl[T](streamName: String,
                                             config: KinesisPublisherConfig,
                                             convert: T => RawRecord) extends EventPublisher[T] {

  // Global context is acceptable here, as it is only used to map Try[PutResult] to Try[Unit]
  import scala.concurrent.ExecutionContext.Implicits.global

  private val rawProducer = RawKinesisStreamPublisher(streamName, config)

  override def publish(event: T): Future[Try[Unit]] = {
    rawProducer.putRecord(convert(event)).map(_.map(_ => Unit))
  }

  override def shutdown(): Unit = rawProducer.shutdown()
}
