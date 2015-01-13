package com.gilt.gfc.kinesis.producer

import java.nio.ByteBuffer

import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import com.gilt.gfc.logging.Loggable

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

case class PartitionKey(value: String) extends AnyVal
case class SequenceNumber(value: String) extends AnyVal
case class PutResult(shardId: String, sequenceNumber: SequenceNumber, attemptCount: Int)

trait KinesisStreamProducer {
  /**
   * Put a single record onto a Kinesis stream.
   * @param data
   * @param partitionKey
   * @param sequenceNumberForOrdering - Optionally specify the sequenceNumber to be used for ordering.
   *                                  see [[http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-add-data-to-stream.html#kinesis-using-sdk-java-putrecord Amazon SDK documentation]]
   *                                  for details.
   * @return
   */
  def putRecord(data: ByteBuffer, partitionKey: PartitionKey, sequenceNumberForOrdering: Option[SequenceNumber] = None): Try[PutResult]
}

object KinesisStreamProducer {
  def apply(streamName: String)(implicit config: KinesisProducerConfig): KinesisStreamProducer = {
    val amazonClient = {
      val client = new AmazonKinesisClient(config.awsCredentialsProvider)
      client.setEndpoint(config.endpoint)
      client.setRegion(Regions.fromName(config.regionName))
      client
    }

    new RetryingStreamProducer(streamName, config, amazonClient)
  }
}

private[producer] class RetryingStreamProducer(streamName: String, config: KinesisProducerConfig, kinesis: AmazonKinesis) extends KinesisStreamProducer with Retry with Loggable {
  override def putRecord(data: ByteBuffer, partitionKey: PartitionKey, sequenceNumberForOrdering: Option[SequenceNumber] = None): Try[PutResult] = {
    retry("putRecord", config) { attemptCount =>
      Try {
        val putRecord = new PutRecordRequest()
        putRecord.setStreamName(streamName)
        putRecord.setData(data)
        putRecord.setPartitionKey(partitionKey.value)
        sequenceNumberForOrdering.foreach(seqnr => putRecord.setSequenceNumberForOrdering(seqnr.value))
        val result = kinesis.putRecord(putRecord)
        PutResult(result.getShardId, SequenceNumber(result.getSequenceNumber), attemptCount)
      }
    }
  }
}

private[producer] trait Retry extends Loggable {
  private[producer] def retry[R](desc: String, config: KinesisProducerConfig)(fn: Int => Try[R]): Try[R] = {
    @tailrec
    def recur(previousAttempt: Try[R], retryCount: Int = 0): Try[R] = {
      previousAttempt match {
        case success @ Success(_) => success
        case failure @ Failure(ex) if retryCount < config.allowedRetriesOnFailure => {
          error(s"$desc failed, attempting retry in $config.retryBackoffDuration", ex)
          Thread.sleep(config.retryBackoffDuration.toMillis)
          val retriedCount = retryCount + 1
          recur(fn(retriedCount + 1), retriedCount)
        }
        case failure => failure
      }
    }

    recur(fn(1))
  }
}
