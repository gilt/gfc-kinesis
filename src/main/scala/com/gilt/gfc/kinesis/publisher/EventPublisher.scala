package com.gilt.gfc.kinesis.publisher

import scala.concurrent.Future
import scala.util.{Success, Failure, Try}

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

  /**
   * Publish a sequence of events sequentially.
   *
   * Ensure that a sequence of events is published in strict order by publishing sequentially, one after another,
   * only publishing a subsequent event once the previous one has definitely been published.
   *
   * On a failure to publish all subsequent events are left unpublished.
   *
   * @param events
   * @return The future number of events that were successfully published.
   */
  def publishSequentially(events: Seq[T]): Future[Int]

  def shutdown(): Unit
}

private[kinesis] class EventPublisherImpl[T](rawProducer: RawKinesisStreamPublisher,
                                             convert: T => RawRecord) extends EventPublisher[T] {

  // Using global context only for facilitating minor conversions, and calling functions - no expensive calls
  // or blocking operations are performed using this context.
  import scala.concurrent.ExecutionContext.Implicits.global

  override def publish(event: T): Future[Try[Unit]] = {
    rawProducer.putRecord(convert(event)).map(_.map(_ => Unit))
  }

  override def publishSequentially(events: Seq[T]): Future[Int] = {
    def recur(remaining: Seq[T], publishedCount: Int): Future[Int] = remaining match {
      case Nil => Future.successful(publishedCount)
      case _ => publish(remaining.head).flatMap {
        case Failure(_) => Future.successful(publishedCount)
        case Success(_) => recur(remaining.tail, publishedCount + 1)
      }
    }
    recur(events, 0)
  }

  override def shutdown(): Unit = rawProducer.shutdown()
}
