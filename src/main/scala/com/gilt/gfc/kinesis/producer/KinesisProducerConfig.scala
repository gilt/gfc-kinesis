package com.gilt.gfc.kinesis.producer

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentialsProvider}

import scala.concurrent.duration.FiniteDuration

trait KinesisProducerConfig {
  /**
   * The AWS region to be used.
   * This must be a valid AWS region
   */
  def regionName: String

  /**
   * The AWS Endpoint for the Kinesis Stream.
   *
   * FIXME can this be optional?? (is it needed if region is specified???
   */
  def endpoint: String

  /**
   * The Credentials Provider to be used to access kinesis
   */
  def awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()

  /**
   * How many retries that are allowed to be applied on a failure to put record(s) to the stream.
   * @return
   */
  def allowedRetriesOnFailure: Int

  /**
   * How long to backoff before attempting a retry to put record(s)
   * @return
   */
  def retryBackoffDuration: FiniteDuration
}
