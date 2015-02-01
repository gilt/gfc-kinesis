package com.gilt.gfc.kinesis.consumer

import scala.concurrent.duration.FiniteDuration

/**
 * Representation of a strategy for applying checkpoints while consuming the kinesis stream/shard.
 *
 * The choice of strategy and frequency of checkpointing may impact the write pressure on the backing AWS DynamoDB
 * table - please ensure to configure that table appropriately from a throughput/capacity perspective.
 */
trait CheckpointingStrategy {
  def onBatchStart(): Boolean = false
  def afterRecord(): Boolean = false
  def afterBatch(lastCheckpointAge: FiniteDuration, uncheckpointedRecordCount: Long): Boolean = false
}

/**
 * Predefined checkpointing strategies.
 */
object CheckpointingStrategy {

  /**
   * Checkpoint periodically based on the "age" of the checkpointer - i.e. the duration since the last checkpoint
   * for this shard/stream.
   *
   * Note, the age here may be approximated based on batches being fed through Kinesis.
   *
   * @param period
   */
  case class Age(period: FiniteDuration) extends CheckpointingStrategy {
    override def afterBatch(age: FiniteDuration, count: Long) = age >= period
  }

  /**
   * Checkpoint periodically based on the record throughput.
   *
   * Note, the throughput here may be approximated based on batches being fed through Kinesis.
   *
   * @param checkpointCount
   */
  case class Throughput(checkpointCount: Long) extends CheckpointingStrategy {
    override def afterBatch(age: FiniteDuration, uncheckpointedCount: Long) = uncheckpointedCount >= checkpointCount
  }

  /**
   * Checkpoint after every batch received from Kinesis.
   */
  case object AfterBatch extends CheckpointingStrategy {
    override def afterBatch(age: FiniteDuration, uncheckpointedCount: Long) = true
  }
}

