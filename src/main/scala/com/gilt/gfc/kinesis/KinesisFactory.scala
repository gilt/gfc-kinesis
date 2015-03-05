package com.gilt.gfc.kinesis

import java.util.concurrent.{Executors, ExecutorService}

import com.gilt.gfc.kinesis.consumer.{EventReceiverImpl, EventReceiver, KinesisConsumerConfig, CheckpointingStrategy}
import com.gilt.gfc.kinesis.publisher._

import scala.concurrent.duration.DurationInt


trait KinesisFactory {
  /**
   * Create a new typed publisher for a given Kineis Stream
   *
   * @param streamName
   * @param config
   * @param convert
   * @tparam T
   * @return
   */
  def newPublisher[T](streamName: String,
                      config: KinesisPublisherConfig,
                      convert: T => RawRecord): EventPublisher[T]

  /**
   * Create a new typed receiver for a given Kinesis Stream.
   * @param streamName
   * @param config
   * @param converter Convert a [[RawRecord]] to an [[Option]] of [[T]] - this allows filtering based on,
   *                  for example, [[com.gilt.gfc.kinesis.publisher.PartitionKey]], etc.
   * @param checkpointingStrategy
   * @param executorService
   * @tparam T
   * @return
   */
  def newReceiver[T](streamName: String,
                     config: KinesisConsumerConfig,
                     converter: RawRecord => Option[T],
                     checkpointingStrategy: CheckpointingStrategy = CheckpointingStrategy.Age(1.minute),
                     executorService: ExecutorService = Executors.newCachedThreadPool()): EventReceiver[T]
}

object KinesisFactory extends KinesisFactory {
  /**
   * Create a new typed publisher for a given Kinesis Stream
   *
   * @param streamName
   * @param config
   * @param convert
   * @tparam T
   * @return
   */
  def newPublisher[T](streamName: String,
                      config: KinesisPublisherConfig,
                      convert: T => RawRecord): EventPublisher[T] = {
    val producer = RawKinesisStreamPublisher(streamName, config)
    new EventPublisherImpl[T](producer, convert)
  }

  /**
   * Create a new typed received for a given Kinesis Stream.
   *
   * @param streamName
   * @param config
   * @param converter Convert a [[RawRecord]] to an [[Option]] of [[T]] - this allows filtering based on,
   *                  for example, [[com.gilt.gfc.kinesis.publisher.PartitionKey]], etc.
   * @param checkpointingStrategy
   * @param executorService
   * @tparam T
   * @return
   */
  def newReceiver[T](streamName: String,
                     config: KinesisConsumerConfig,
                     converter: RawRecord => Option[T],
                     checkpointingStrategy: CheckpointingStrategy = CheckpointingStrategy.Age(1.minute),
                     executorService: ExecutorService = Executors.newCachedThreadPool()): EventReceiver[T] = {
    new EventReceiverImpl[T](streamName, config, converter, checkpointingStrategy, executorService)
  }
}