package com.gilt.gfc.kinesis.publisher

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.{PutRecordRequest, PutRecordResult}
import com.gilt.gfc.kinesis.common.{SequenceNumber, ShardId}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.Matchers.{any, argThat}
import org.mockito.Mockito.{doReturn, doThrow, times, verify}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class RawKinesisStreamPublisherSpec extends FlatSpec with Matchers with MockitoSugar with ScalaFutures {
  private case class PutRecordRequestMatcher(streamName: String, data: Array[Byte], partitionKey: PartitionKey, sequenceNumberForOrdering: Option[SequenceNumber] = None) extends BaseMatcher[PutRecordRequest] {
    override def matches(o: scala.Any): Boolean = {
      o match {
        case request: PutRecordRequest =>
          request.getPartitionKey == partitionKey.value &&
          request.getStreamName == streamName &&
          request.getData.array== data &&
          sequenceNumberForOrdering.fold(true)(_.value == request.getSequenceNumberForOrdering)
        case _ => false
      }
    }

    override def describeTo(description: Description): Unit = {
      description.appendText(s"PutRecordRequest(streamName = $streamName, partitionKey = $partitionKey, data = $data, sequenceNumberForOrdering = $sequenceNumberForOrdering")
    }
  }

  "A RawKinesisStreamProducer" should "successfully put record" in {
    val kinesis = mock[AmazonKinesis]

    val config = mock[KinesisPublisherConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(10.milliseconds).when(config).retryBackoffDuration

    doReturn(new PutRecordResult().withShardId("testshard1").withSequenceNumber("myseq123"))
      .when(kinesis).putRecord(any[PutRecordRequest])

    val iut = new RetryingStreamPublisher("streamname1", config, kinesis)

    val rawRecord = RawRecord("hi earth".getBytes, PartitionKey("some partition key"))

    val futureResult = iut.putRecord(rawRecord)

    whenReady(futureResult) { result =>
      result.isSuccess should be(true)
      result.get should be(PutResult(ShardId("testshard1"), SequenceNumber("myseq123"), 1))
    }

    verify(kinesis, times(1)).putRecord(any[PutRecordRequest])
  }

  it should "exercise all allowed retries on continual failure" in {
    val kinesis = mock[AmazonKinesis]

    val config = mock[KinesisPublisherConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(5.milliseconds).when(config).retryBackoffDuration

    doThrow(new AmazonServiceException("testing")).when(kinesis).putRecord(any[PutRecordRequest])

    val iut = new RetryingStreamPublisher("streamname1", config, kinesis)

    val bytes = "dia dhuit an domhain".getBytes
    val futureResult = iut.putRecord(RawRecord(bytes, PartitionKey("somepartitionkey")))
    whenReady(futureResult.failed) { result =>
      true
    }

    verify(kinesis, times(11)).putRecord(argThat(PutRecordRequestMatcher("streamname1", bytes, PartitionKey("somepartitionkey"))))
  }

  it should "perform retries until successfully putting record (while within allowed retry limit" in {
    val kinesis = mock[AmazonKinesis]

    val config = mock[KinesisPublisherConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(10.milliseconds).when(config).retryBackoffDuration

    doThrow(new AmazonServiceException("testing"))
    .doThrow(new AmazonServiceException("testing"))
    .doReturn(new PutRecordResult().withShardId("testshard1").withSequenceNumber("myseq123"))
      .when(kinesis).putRecord(any[PutRecordRequest])

    val iut = new RetryingStreamPublisher("streamname1", config, kinesis)

    val bytes = "hello world".getBytes
    val futureResult = iut.putRecord(RawRecord(bytes, PartitionKey("somepartitionkey")))
    whenReady(futureResult) { result =>
      result.isSuccess should be(true)
      result.get should be(PutResult(ShardId("testshard1"), SequenceNumber("myseq123"), 3))
    }

    verify(kinesis, times(3)).putRecord(argThat(PutRecordRequestMatcher("streamname1", bytes, PartitionKey("somepartitionkey"))))
  }
}
