package com.gilt.gfc.kinesis.consumer

import java.util.UUID

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.gilt.gfc.kinesis.common.BaseConfig
import com.gilt.gfc.logging.Loggable

import scala.concurrent.duration._

/**
 * Configuration for access around a Kinesis Stream.
 *
 * There are some (arguably) reasonable defaults here.
 */
trait KinesisConsumerConfig extends BaseConfig with Loggable {
  /**
   * Name of the Amazon Kinesis application
   */
  def appName: String

  /**
   * Optionally explicitly define the endpoint to be used for Kinesis.
   *
   * If specified, this setting will be used to configure the Amazon Kinesis client to
   * read from setting, overwriting the configured (as per [[regionName]]) region name (but '''only''' for Kinesis -
   * the DynamoDB and CloudWatch will still use the configured region name.)
   *
   * Defaults to None
   *
   * @return
   */
  override def kinesisEndpoint: Option[String] = None

  /**
   * Allow for overriding the [[awsCredentialsProvider]] for connection to Dynamo.
   *
   * This allows the Kinesis stream and lease/checkpoint DynamoDB table to exist in different accounts, etc.
   *
   * Defaults to None
   *
   * @return
   */
  def dynamoOverrideAwsCredentialsProvider: Option[AWSCredentialsProvider] = None

  /**
   * Used to identify different worker processes - needs to be different for each instance, not just application.
   *
   * This value is logged (INFO) when a configuration for a stream is created - this is useful to assist inspection
   * of the contents of the associated dynamoDB table.
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
   * One of `LATEST` or `TRIM_HORIZON`. The Amazon Kinesis Client Library will start
   * fetching records from this position when the application starts up if there are no checkpoints. If there
   * are checkpoints, we will process records from the checkpoint position.
   *
   * @return defaults to `TRIM_HORIZON`
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
   * The underlying client effectively polls the kinesis stream server periodically - when there are no records
   * present to be passed to the application this is effectively an empty batch - by default not all these get
   * passed to the application.
   *
   * '''Note''', in the case where an application does not checkpoint every batch, empty batches may still
   * be passed to the application, even if this specifies `false` - however, in that case, empty batches will only
   * be passed when there has been records passed since the last checkpoint - if no records have been passed since last
   * checkpoint then no empty batches will be passed on.
   *
   * It is not expected, or necessary, to configure this to `true` - however, it may be useful for certain types of
   * applications to process
   *
   * Defaults to `false`
   *
   * @return
   */
  def processAllEmptyBatches: Boolean = false

  /**
   * Create a Kinesis Client Lib Configuration. This is used internally when configuring the Java Kinesis client connections.
   *
   * The consumer associated with this confguration will be identified by `s"$appName-$streamName"` (this includes
   * the DynamoDB table, etc.)
   *
   * @param streamName
   * @return
   */
  final def createStreamConfiguration(streamName: String): KinesisClientLibConfiguration = {
    val wid = workerId
    val aname = appName
    info(s"Application $aname creating a stream configuration for $streamName, with workerId $wid")

    val baseConfig = new KinesisClientLibConfiguration(
      s"$aname-$streamName",
      streamName,
      awsCredentialsProvider,
      dynamoOverrideAwsCredentialsProvider.getOrElse(awsCredentialsProvider),
      awsCredentialsProvider,
      wid
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
    val endpointConfigured = kinesisEndpoint.fold(baseConfig)(baseConfig.withKinesisEndpoint)

    // Optional common client configuration
    val fullConfig = awsClientConfig.fold(endpointConfigured)(endpointConfigured.withCommonClientConfig)

    fullConfig
  }
}
