package com.gilt.gfc.kinesis

import java.util.concurrent.{Executors, ExecutorService}

import scala.concurrent.duration.DurationInt

import com.gilt.gfc.kinesis.consumer.{CheckpointingStrategy, KinesisConsumerConfig}
import com.gilt.gfc.kinesis.producer.KinesisProducerConfig
import com.gilt.gfc.kinesis.producer.raw.RawRecord


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
                      config: KinesisProducerConfig,
                      convert: T => RawRecord): EventPublisher[T]

  /**
   * Create a new typed received for a given Kinesis Stream.
   * @param streamName
   * @param config
   * @param converter
   * @param checkpointingStrategy
   * @param executorService
   * @tparam T
   * @return
   */
  def newReceiver[T](streamName: String,
                     config: KinesisConsumerConfig,
                     converter: Array[Byte] => T,
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
                      config: KinesisProducerConfig,
                      convert: T => RawRecord): EventPublisher[T] = {
    new EventPublisherImpl[T](streamName, config, convert)
  }

  /**
   * Create a new typed received for a given Kinesis Stream.
   *
   * @param streamName
   * @param config
   * @param converter
   * @param checkpointingStrategy
   * @param executorService
   * @tparam T
   * @return
   */
  def newReceiver[T](streamName: String,
                     config: KinesisConsumerConfig,
                     converter: Array[Byte] => T,
                     checkpointingStrategy: CheckpointingStrategy = CheckpointingStrategy.Age(1.minute),
                     executorService: ExecutorService = Executors.newCachedThreadPool()): EventReceiver[T] = {
    new EventReceiverImpl[T](streamName, config, converter, checkpointingStrategy, executorService)
  }
}