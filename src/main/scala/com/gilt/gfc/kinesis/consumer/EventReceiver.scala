package com.gilt.gfc.kinesis.consumer

import java.util.concurrent.{ExecutorService, Executors}

import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.kinesis.common.SequenceNumber
import com.gilt.gfc.kinesis.publisher.RawRecord
import com.gilt.gfc.logging.Loggable

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

/**
 * Receiver of events of a certain type from Kinesis.
 *
 * Each Receiver can have zero or more consumers registered. Each consumer
 * will passed each event in the order of registration, unless there is an error, which will
 * be passed to the configured error handler (default error handler simply logs ERROR)
 *
 * @tparam T
 */
trait EventReceiver[T] {
  /**
   * Register a consumer defined as a function.
   *
   * A single consumer function will only be registered once, if it is already registered it will silently be ignored
   * on subsequent attempts.
   *
   * @param consumer
   * @return True if added, false if not.
   */
  def registerConsumer(consumer: T => Unit): Boolean

  /**
   * Unregister a previosuly registered consumer function.
   *
   * @param consumer
   * @return True if removed, false if not.
   */
  def unregisterConsumer(consumer: T => Unit): Boolean

  /**
   * Replace the error handler.
   *
   * The error handler is called for exceptions thrown by application provided conversion, or handler functions.
   *
   * The default error handler simply logs exceptions as ERROR.
   *
   * @param handler
   */
  def onError(handler: Exception => Unit): Unit

  def start(): Unit

  def shutdown(): Unit
}

/**
 *
 * @param streamName
 * @param config
 * @param converter Convert a [[RawRecord]] to an [[Option]] of [[T]] - this allows filtering based on,
 *                  for example, [[com.gilt.gfc.kinesis.publisher.PartitionKey]], etc.
 * @param checkpointingStrategy
 * @param executorService
 * @tparam T
 */
private[kinesis] class EventReceiverImpl[T](streamName: String,
                                            config: KinesisConsumerConfig,
                                            converter: RawRecord => Option[T],
                                            checkpointingStrategy: CheckpointingStrategy = CheckpointingStrategy.Age(1.minute),
                                            executorService: ExecutorService = Executors.newCachedThreadPool()) extends EventReceiver[T] with Loggable {

  @volatile private var errorHandler: Exception => Unit = defaultErrorHandler _

  private val consumersMutex = new Object
  private val consumers = mutable.ArrayBuffer[T => Unit]()

  private val underlyingConsumer = RawKinesisStreamConsumer(streamName, config, executorService)(processBatch)


  override def onError(handler: (Exception) => Unit): Unit = {
    errorHandler = handler
  }

  override def registerConsumer(consumer: (T) => Unit): Boolean = {
    consumersMutex.synchronized {
      if (!consumers.contains(consumer)) {
        consumers += consumer
        true
      } else {
        false
      }
    }
  }

  override def unregisterConsumer(consumer: (T) => Unit): Boolean = {
    consumersMutex.synchronized {
      if (consumers.contains(consumer)) {
        consumers -= consumer
        true
      } else {
        false
      }
    }
  }

  override def start(): Unit = underlyingConsumer.start()

  override def shutdown(): Unit = underlyingConsumer.shutdown()

  private def defaultErrorHandler(ex: Exception): Unit = {
    error("Unexpected issue in application logic while converting or consuming record", ex)
  }

  private def processBatch(batch: Seq[Record], checkpoint: Checkpoint): Unit = {

    // Use a copy of the seq of consumers, to allow dynamic register/unregister to take place during this processing
    // without fear of deadlock, etc.
    val localConsumers = consumersMutex.synchronized(Seq(consumers:_*))

    def processRecord(record: Record): Unit = {

      def handleEvent(event: T): Unit = {
        Try {
          localConsumers.foreach(_(event))
        }.recover {
          case e: Exception => errorHandler(e)
        }
      }

      Try {
        converter(RawRecord(record)).foreach { converted =>
          handleEvent(converted)
          if (checkpointingStrategy.afterRecord(checkpoint.shardId, SequenceNumber(record.getSequenceNumber))) checkpoint()
        }
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
