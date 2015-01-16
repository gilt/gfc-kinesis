package com.gilt.gfc.kinesis.producer

import com.gilt.gfc.kinesis.common.BaseConfig

import scala.concurrent.duration._

trait KinesisProducerConfig extends BaseConfig {
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
   * The size of the internal thread pool for placing records onto the kinesis stream. This thread pool is also
   * used for scheduling and executing retries.
   *
   * @return Defaults to 50 (which mirrors Amazon's Java AmazonKinesisAsyncClient default configuration at time of writing)
   */
  def streamPlacementThreadCount: Int = 50
}
