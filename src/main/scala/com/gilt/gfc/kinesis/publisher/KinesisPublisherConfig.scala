package com.gilt.gfc.kinesis.publisher

import com.gilt.gfc.kinesis.common.BaseConfig

import scala.concurrent.duration._

trait KinesisPublisherConfig extends BaseConfig {
  /**
   * How many retries that are allowed to be applied on a failure to put record(s) to the stream.
   * @return Defaults to 3 retries
   */
  def allowedRetriesOnFailure: Int = 3

  /**
   * How long to backoff before attempting a retry to put record(s)
   * @return Defaults to 3 seconds
   */
  def retryBackoffDuration: FiniteDuration = 3.seconds

  /**
   * The maximum number of underlying connections to kinesis.
   *
   * By Default this matches [[streamPlacementThreadCount]] - in general these numbers should match, the thread count
   * could be larger, as that thread pool is also used for retries.
   *
   * Note, specifying a [[BaseConfig.awsClientConfig]] will cause this value to be ignored (with the maxConnections value
   * of the specified [[com.amazonaws.ClientConfiguration]] being used directly.
   *
   * @return
   */
  def maxConnectionCount: Int = 50

  /**
   * The size of the internal thread pool for placing records onto the kinesis stream. This thread pool is also
   * used for scheduling and executing retries. For efficient use of resources this should be equal, or greater than,
   * [[maxConnectionCount]]
   *
   * @return Defaults to the maximum number of connections, [[maxConnectionCount]]
   */
  def streamPlacementThreadCount: Int = maxConnectionCount
}
