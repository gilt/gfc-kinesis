package com.gilt.gfc.kinesis.consumer

import scala.concurrent.duration.FiniteDuration

/**
 * Allows application to checkpoint a stream's shard at latest read point, or at a specific sequence number.
 *
 * Also provided is tracking information on the last checkpoint (its age, and also the number of records passed to
 * application since last checkpoint) of the associated shard.
 */
trait Checkpoint {
  /**
   * This method will checkpoint the progress at the last data record that was delivered to the consumer for this shard.
   * In steady state, applications should checkpoint periodically (e.g. once every 5 minutes).
   * Calling this API too frequently can slow down the application (because it puts pressure on the underlying
   * checkpoint storage layer).
   */
  def apply(): Unit

  /**
   * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
   * {@link #apply()} but provides the ability to specify the sequence number at which to checkpoint.
   *
   * @param sequenceNumber
   */
  def apply(sequenceNumber: String): Unit

  /**
   * The duration since the last checkpoint. This can be used by the application to implement less frequent periodic
   * checkpointing.
   *
   * @return
   */
  def age(): FiniteDuration

  /**
   * The number of records that have been passed to the application (for the shard in question) since the last checkpoint
   * @return
   */
  def size(): Long
}