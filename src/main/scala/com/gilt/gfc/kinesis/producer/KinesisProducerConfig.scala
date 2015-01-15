package com.gilt.gfc.kinesis.producer

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentialsProvider}

import scala.concurrent.duration._

trait KinesisProducerConfig {
  /**
   * The AWS region to be used.
   * This must be a valid AWS region
   *
   * At minimal, this or [[endpoint]] must be set - both can be specified.
   *
   * @return defaults to None
   */
  def regionName: Option[String] = None

  /**
   * The AWS Endpoint for the Kinesis Stream.
   *
   * At minimal, this or [[regionName]] must be set - both can be specified.
   *
   * @return defaults to None
   */
  def endpoint: Option[String] = None

  /**
   * The Credentials Provider to be used to access kinesis
   */
  def awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()

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
