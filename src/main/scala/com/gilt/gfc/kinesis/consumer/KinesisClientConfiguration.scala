package com.gilt.gfc.kinesis.consumer

import java.util.UUID

import scala.concurrent.duration._

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}

/**
 * Configuration for access around a Kinesis Stream.
 *
 * There are some (arguably) reasonable defaults here.
 */
trait KinesisClientConfiguration {
  /**
   * Name of the Amazon Kinesis application
   */
  def appName: String

  /**
   * Specify the Amazon region name to be used for each of Kinesis, DynamoDB (for lease table) and CloudWatch metrics.
   *
   * Defaults to "us-east-1"
   * @return
   */
  def regionName: String = "us-east-1"

  /**
   * Optionally explicitly define the endpoint to be used for Kinesis.
   *
   * If specified, this setting will be used to configure the Amazon Kinesis client to
   * read from this specified endpoint, overwriting the configured region name (but ONLY for Kinesis -
   * the DynamoDB and CloudWatch will still use the configured region name.
   *
   * @return
   */
  def kinesisEndpoint: Option[String] = None

  /**
   * Get the AWS CredentialsProvider for Kinesis, etc., access.
   *
   * Defaults to DefaultAWSCredentialsProviderChain
   *
   * @return
   */
  def awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()

  /**
   * Allow for overriding the {@link #awsCredentialsProvider} for connection to Dynamo.
   *
   * This allows the Kinesis stream and lease/checkpoint DynamoDB table to exist in different accounts, etc.
   *
   * @return
   */
  def dynamoOverrideAwsCredentialsProvider: Option[AWSCredentialsProvider] = None

  /**
   * Used to identify different worker processes - needs to be different for each instance, not just application.
   *
   * Defaults to the string value of a random UUID.
   * @return
   */
  def workerId: String = UUID.randomUUID.toString

  /**
   * Lease duration (leases not renewed within this period will be claimed by others)
   *
   * @return defaults to 30 seconds
   */
  def leaseFailoverTime: FiniteDuration = 30.seconds

  /**
   * Wait for this long between polls to check if parent shards are done
   *
   * @return defaults to 20 seconds
   */
  def parentShardPollInterval: FiniteDuration = 20.seconds

  /**
   * Idle time between calls to fetch data from Kinesis
   *
   * @return defaults to 10 seconds
   */
  def idleTimeBetweenReads: FiniteDuration = 10.second

  /**
   * Time between tasks to sync leases and Kinesis shards
   *
   * @return defaults to 1 minute
   */
  def shardSyncInterval: FiniteDuration = 1.minute

  /**
   * Backoff period when tasks encounter an exception
   *
   * @return defaults to 20 seconds
   */
  def taskBackoffTime: FiniteDuration = 20.seconds

  /**
   * Metrics are buffered for at most this long before publishing to CloudWatch
   *
   * @return defaults to 20 seconds
   */
  def metricsBufferTime: FiniteDuration = 20.seconds

  /**
   * Max number of metrics to buffer before publishing to CloudWatch
   *
   * @return defaults to 1000
   */
  def metricsMaxQueueSize: Int = 1000

  /**
   * Maximum number of records in a batch.
   *
   * @return defaults to 1000
   */
  def maxBatchSize: Int = 1000

  /**
   * One of LATEST or TRIM_HORIZON. The Amazon Kinesis Client Library will start
   * fetching records from this position when the application starts up if there are no checkpoints. If there
   * are checkpoints, we will process records from the checkpoint position.
   *
   * @return defaults to TRIM_HORIZON
   */
  def initialPositionInStream: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON

  /**
   * How long to backoff after a communication, etc., error issuing a Checkpoint on a stream/shard, before automatically retrying.
   *
   * Defaults to 30 seconds.
   * @return
   */
  def checkpointFailBackoff: FiniteDuration = 30.seconds

  /**
   * How often to retry a checkpoint, on failure, before completely failing.
   *
   * Defaults to 3 attempts.
   *
   * @return
   */
  def checkpointFailRetryCount: Int = 3

  /**
   * Whether every empty batch should be passed to the application.
   *
   * Note, in the case where an application does not checkpoint every batch empty batches may still
   * be passed to the application, even if this specifies False - however, in that case, empty batches will only
   * be passed when there has been records passed since the last checkpoint - if no records have been passed, then
   * no empty batches will be passed on, unless this specified by True here.
   * @return
   */
  def processAllEmptyBatches: Boolean = false

  /**
   * Create a Kinesis Client Lib Configuration. This is used internally when configuring the Java Kinesis client connections.
   *
   * @param streamName
   * @return
   */
  final def createStreamConfiguration(streamName: String): KinesisClientLibConfiguration = {
    val config = new KinesisClientLibConfiguration(
      s"$appName-$streamName",
      streamName,
      awsCredentialsProvider,
      dynamoOverrideAwsCredentialsProvider.getOrElse(awsCredentialsProvider),
      awsCredentialsProvider,
      workerId
    ).withRegionName(regionName)
      .withFailoverTimeMillis(leaseFailoverTime.toMillis)
      .withMaxRecords(maxBatchSize)
      .withInitialPositionInStream(initialPositionInStream)
      .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReads.toMillis)
      .withCallProcessRecordsEvenForEmptyRecordList(true) // Regardless of the value of "processAllEmptyBatches" to allow passing batches when pending checkpoint.
      .withCleanupLeasesUponShardCompletion(false)
      .withParentShardPollIntervalMillis(parentShardPollInterval.toMillis)
      .withShardSyncIntervalMillis(shardSyncInterval.toMillis)
      .withTaskBackoffTimeMillis(taskBackoffTime.toMillis)
      .withMetricsBufferTimeMillis(metricsBufferTime.toMillis)
      .withMetricsMaxQueueSize(metricsMaxQueueSize)

    // Optional kinesis endpoint
    kinesisEndpoint.fold(config)(config.withKinesisEndpoint)
  }
}
