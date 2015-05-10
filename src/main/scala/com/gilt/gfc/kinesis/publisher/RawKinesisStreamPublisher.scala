package com.gilt.gfc.kinesis.publisher

import java.nio.ByteBuffer
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.amazonaws.ClientConfiguration
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.{Record, PutRecordResult, PutRecordRequest}
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClient}
import com.gilt.gfc.kinesis.common.{SequenceNumber, ShardId}
import com.gilt.gfc.logging.Loggable

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait RawRecord {
  def data: Array[Byte]
  def partitionKey: PartitionKey
}

object RawRecord {
  def apply(data: Array[Byte], partitionKey: PartitionKey): RawRecord = new SimpleRawRecord(data, partitionKey)
  def apply(awsRecord: Record): RawRecord = new AwsWrappingRawRecord(awsRecord)
}

private[publisher] class SimpleRawRecord(override val data: Array[Byte], override val partitionKey: PartitionKey) extends RawRecord

private[publisher] class AwsWrappingRawRecord(awsRecord: Record) extends RawRecord {
  override lazy val data: Array[Byte] = awsRecord.getData.array
  override lazy val partitionKey: PartitionKey = PartitionKey(awsRecord.getPartitionKey)
}

trait RawKinesisStreamPublisher {
  /**
   * Put a single record onto a Kinesis stream.
   *
   * This method returns a Future of the placement of the single record.
   *
   * It is the responsibility of the caller to ensure correct sequencing/serialisation guarantees - either by explicitly
   * awaiting on the Future's result, or through Future composition (through Future.flatMap, etc.). This still holds even
   * if the concurrency configuration for this producer is limited to 1, as retries are scheduled asynchronously.
   *
   * @param record The record to be put on the stream.
   * @param sequenceNumberForOrdering - Optionally specify the sequenceNumber to be used for ordering.
   *                                  see [[http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-add-data-to-stream.html#kinesis-using-sdk-java-putrecord Amazon SDK documentation]]
   *                                  for details.
   * @return
   */
  def putRecord(record: RawRecord, sequenceNumberForOrdering: Option[SequenceNumber] = None): Future[Try[PutResult]]

  def shutdown(): Unit
}

object RawKinesisStreamPublisher {
  def apply(streamName: String, config: KinesisPublisherConfig): RawKinesisStreamPublisher = {
    val amazonClient = {
      val clientConfig = config.awsClientConfig.getOrElse {
        new ClientConfiguration().withMaxConnections(config.maxConnectionCount)
      }

      val client = new AmazonKinesisAsyncClient(config.awsCredentialsProvider, clientConfig)
      client.setRegion(Regions.fromName(config.regionName))
      config.kinesisEndpoint.foreach(client.setEndpoint)
      client
    }

    new RetryingStreamPublisher(streamName, config, amazonClient)
  }
}

private[publisher] class RetryingStreamPublisher(streamName: String, config: KinesisPublisherConfig, kinesis: AmazonKinesisAsync) extends RawKinesisStreamPublisher with Retry with Loggable {

  // Only need small pool here - used only for scheduling retries, and small mapping, etc. - no real work.
  override val scheduledExecutor = Executors.newScheduledThreadPool(5)
  private implicit val executionContext = ExecutionContext.fromExecutor(scheduledExecutor)

  override def shutdown(): Unit = kinesis.shutdown()

  override def putRecord(record: RawRecord, sequenceNumberForOrdering: Option[SequenceNumber] = None): Future[Try[PutResult]] = {
    futureRetry("putRecord", config) { attemptCount =>
      Try {
        val putRecord = new PutRecordRequest()
        putRecord.setStreamName(streamName)
        putRecord.setData(ByteBuffer.wrap(record.data))
        putRecord.setPartitionKey(record.partitionKey.value)
        sequenceNumberForOrdering.foreach(seqnr => putRecord.setSequenceNumberForOrdering(seqnr.value))

        val asyncAdapter = AsyncHandlerAdapter(attemptCount)
        kinesis.putRecordAsync(putRecord, asyncAdapter)
        asyncAdapter.future
      }.recover {
        case ex =>
          Future.successful(Failure(ex))
      }.get
    }
  }
}

private[publisher] case class AsyncHandlerAdapter(attempt: Int) extends AsyncHandler[PutRecordRequest, PutRecordResult] {
  private val promise = Promise[Try[PutResult]]

  def future = promise.future

  override def onError(exception: Exception): Unit = promise.success(Failure(exception))

  override def onSuccess(request: PutRecordRequest, result: PutRecordResult): Unit = {
    promise.success(Success(PutResult(ShardId(result.getShardId), SequenceNumber(result.getSequenceNumber), attempt)))
  }
}

private[publisher] trait Retry extends Loggable {
  def scheduledExecutor: ScheduledExecutorService

  private[publisher] def futureRetry[R](desc: String, config: KinesisPublisherConfig)
                                       (fn: Int => Future[Try[R]])
                                       (implicit ec: ExecutionContext): Future[Try[R]] = {
    def recur(previous: Try[R], retryCount: Int): Future[Try[R]] = {
      previous match {
        case success@Success(_) => {
          Future.successful(success)
        }
        case failure@Failure(ex) if retryCount < config.allowedRetriesOnFailure => {
          error(s"$desc failed, attempting retry in ${config.retryBackoffDuration}", ex)

          val retriedCount = retryCount + 1
          after(config.retryBackoffDuration) {
            fn(retriedCount + 1)
          }.flatMap(identity).flatMap(recur(_, retriedCount))
        }
      }
    }

    fn(1).flatMap(recur(_, 0))
  }

  private[publisher] def after[R](duration: FiniteDuration)(fn: => R): Future[R] = {
    val promise = Promise[R]
    scheduledExecutor.schedule(new Runnable { def run() = promise.success(fn) }, duration.toMillis, TimeUnit.MILLISECONDS)
    promise.future
  }
}

