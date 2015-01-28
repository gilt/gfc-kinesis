package com.gilt.gfc.kinesis.consumer

import java.util.concurrent.{ExecutorService, Executors}

import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.kinesis.consumer.raw.{RawKinesisStreamConsumer, Checkpoint}
import com.gilt.gfc.logging.Loggable

import scala.concurrent.duration.DurationInt
import scala.util.Try

/**
 * Consume a kinesis stream of [T] events.
 *
 * Records from this stream are converted to application specific type (using provided "reader") and passed to the
 * application on an event-by-event baisis.
 *
 * @param streamName
 * @param config
 * @param converter Application provided function to read bytes, and constructing individual T event - one event
 *                     per call. Any (NonFatal) exception thrown by this method will be logged (as ERROR),
 *                     but otherwise silently passed over.
 * @param checkpointingStrategy defaults to periodically checkpointing, every 1 minute. This checkpointing strategy is
 *                              followed regardless of exceptions encountered when calling application provided functions
 *                              (including converter, event-handler and/or error handler)
 * @param executorService The executorService used to allocate threads to the underlying Amazon library - concurrency
 *                        (or lack thereof) consuming multiple shards is controlled through this - defaults to
 *                        [java.util.concurrent.Executors.newCachedThreadPool]
 * @param eventHandler The event handler.
 * @tparam T
 */
case class KinesisStreamConsumer[T](streamName: String,
                                    config: KinesisConsumerConfig,
                                    converter: Array[Byte] => T,
                                    checkpointingStrategy: CheckpointingStrategy = CheckpointingStrategy.Age(1.minute),
                                    executorService: ExecutorService = Executors.newCachedThreadPool())
                                   (eventHandler: T => Unit) extends Loggable {

  /**
   * Start consuming records.
   */
  def start(): Unit = underlyingConsumer.start()

  /**
   * shutdown this consumer.
   */
  def shutdown(): Unit = underlyingConsumer.shutdown()

  /**
   * Replace the error handler.
   *
   * The error handler is called for exceptions thrown by application provided conversion, or handler functions.
   *
   * The default error handler simply logs exceptions as ERROR.
   *
   * @param handler
   */
  def onError(handler: Exception => Unit): Unit = {
    errorHandler = handler
  }

  @volatile private var errorHandler: Exception => Unit = defaultErrorHandler _

  private def defaultErrorHandler(ex: Exception): Unit = {
    error("Unexpected issue in application logic while converting or consuming record", ex)
  }

  private val underlyingConsumer = RawKinesisStreamConsumer(streamName, config, executorService)(processBatch)

  private def processBatch(batch: Seq[Record], checkpoint: Checkpoint): Unit = {

    def processRecord(record: Record): Unit = {

      def onConvertedRecord(converted: T): Unit = {
        eventHandler(converted)
        if (checkpointingStrategy.afterRecord()) checkpoint()
      }

      Try {
        onConvertedRecord(converter(record.getData.array))
      }.recoverWith {
        case ex: Exception => Try(errorHandler(ex)).recover {
          case ex2 => error("Unexpected error calling error handler for error during application conversion/consumption of record", ex2)
        }
      }
    }

    if (checkpointingStrategy.onBatchStart()) checkpoint()
    batch.foreach(processRecord)
    if (checkpointingStrategy.afterBatch(checkpoint.age, checkpoint.size)) checkpoint()
  }
}