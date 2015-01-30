package com.gilt.gfc.kinesis

import java.util.concurrent.{Executors, ExecutorService}

import scala.collection.mutable

import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.kinesis.consumer.raw.{Checkpoint, RawKinesisStreamConsumer}
import com.gilt.gfc.logging.Loggable

import scala.concurrent.duration.DurationInt

import com.gilt.gfc.kinesis.consumer.{CheckpointingStrategy, KinesisConsumerConfig}

import scala.util.Try
import scala.util.control.NonFatal

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
   */
  def registerConsumer(consumer: T => Unit): Unit

  /**
   * Unregister a previosuly registered consumer function.
   *
   * @param consumer
   */
  def unregisterConsumer(consumer: T => Unit): Unit

  /**
   * Register a consumer
   *
   * A single consumer will only be registered once, if it is already registered it will silently be ignored
   * on subsequent attempts.
   *
   * @param consumer
   */
  def registerConsumer(consumer: EventConsumer[T]): Unit

  /**
   * Unregister a previously registered consumer.
   *
   * @param consumer
   */
  def unregisterConsumer(consumer: EventConsumer[T]): Unit

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

private[kinesis] class EventReceiverImpl[T](streamName: String,
                                            config: KinesisConsumerConfig,
                                            converter: Array[Byte] => T,
                                            checkpointingStrategy: CheckpointingStrategy = CheckpointingStrategy.Age(1.minute),
                                            executorService: ExecutorService = Executors.newCachedThreadPool()) extends EventReceiver[T] with Loggable {


  @volatile private var errorHandler: Exception => Unit = defaultErrorHandler _

  private val consumers = mutable.ArrayBuffer[Either[T => Unit, EventConsumer[T]]]()
  private val consumersMutex = new Object

  private val underlyingConsumer = RawKinesisStreamConsumer(streamName, config, executorService)(processBatch)


  override def onError(handler: (Exception) => Unit): Unit = {
    errorHandler = handler
  }

  override def registerConsumer(consumer: (T) => Unit): Unit = register(Left(consumer))
  override def registerConsumer(consumer: EventConsumer[T]): Unit = register(Right(consumer))

  override def unregisterConsumer(consumer: (T) => Unit): Unit = unregister(Left(consumer))
  override def unregisterConsumer(consumer: EventConsumer[T]): Unit = unregister(Right(consumer))

  private def register(consumer: Either[T => Unit, EventConsumer[T]]): Unit = {
    consumersMutex.synchronized {
      if (!consumers.contains(consumer)) {
        consumers += consumer
      }
    }
  }

  private def unregister(consumer: Either[T => Unit, EventConsumer[T]]): Unit = {
    consumersMutex.synchronized {
      if (consumers.contains(consumer)) {
        consumers -= consumer
      }
    }
  }

  override def start(): Unit = underlyingConsumer.start()

  override def shutdown(): Unit = underlyingConsumer.shutdown()

  private def defaultErrorHandler(ex: Exception): Unit = {
    error("Unexpected issue in application logic while converting or consuming record", ex)
  }

  private def processBatch(batch: Seq[Record], checkpoint: Checkpoint): Unit = {

    def processRecord(record: Record): Unit = {

      def onConvertedRecord(converted: T): Unit = {
        handleEvent(converted)
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

  private def handleEvent(event: T): Unit = {
    try {
      consumersMutex.synchronized {
        consumers.foreach { eitherConsumer =>
          eitherConsumer.fold(_(event), _.onEvent(event))
        }
      }
    } catch {
      case NonFatal(e: Exception) => errorHandler(e)
    }
  }
}
