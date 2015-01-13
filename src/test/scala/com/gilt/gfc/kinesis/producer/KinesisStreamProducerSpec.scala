package com.gilt.gfc.kinesis.producer

import java.nio.ByteBuffer

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.{PutRecordRequest, PutRecordResult}
import org.hamcrest.{Description, BaseMatcher}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.mockito.Mockito.{doReturn, doThrow, verify, times}
import org.mockito.Matchers.{any, anyString, argThat}

class KinesisStreamProducerSpec extends FlatSpec with Matchers with MockitoSugar with ScalaFutures {
  private case class PutRecordRequestMatcher(streamName: String, data: ByteBuffer, partitionKey: PartitionKey, sequenceNumberForOrdering: Option[SequenceNumber] = None) extends BaseMatcher[PutRecordRequest] {
    override def matches(o: scala.Any): Boolean = {
      o match {
        case request: PutRecordRequest =>
          request.getPartitionKey == partitionKey.value &&
          request.getStreamName == streamName &&
          request.getData == data &&
          sequenceNumberForOrdering.fold(true)(_.value == request.getSequenceNumberForOrdering)
        case _ => false
      }
    }

    override def describeTo(description: Description): Unit = {
      description.appendText(s"PutRecordRequest(streamName = $streamName, partitionKey = $partitionKey, data = $data, sequenceNumberForOrdering = $sequenceNumberForOrdering")
    }
  }

  "A KinesisStreamProducer" should "successfully put record" in {
    val kinesis = mock[AmazonKinesis]

    val config = mock[KinesisProducerConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(10.milliseconds).when(config).retryBackoffDuration

    doReturn(new PutRecordResult().withShardId("testshard1").withSequenceNumber("myseq123"))
      .when(kinesis).putRecord(any[PutRecordRequest])

    val iut = new RetryingStreamProducer("streamname1", config, kinesis)

    val futureResult = iut.putRecord(mock[ByteBuffer], PartitionKey("somepartitionkey"))

    whenReady(futureResult) { result =>
      result.isSuccess should be(true)
      result.get should be(PutResult("testshard1", SequenceNumber("myseq123"), 1))
    }

    verify(kinesis, times(1)).putRecord(any[PutRecordRequest])
  }

  it should "exercise all allowed retries on continual failure" in {
    val kinesis = mock[AmazonKinesis]

    val config = mock[KinesisProducerConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(10.milliseconds).when(config).retryBackoffDuration

    doThrow(new AmazonServiceException("testing")).when(kinesis).putRecord(any[PutRecordRequest])

    val iut = new RetryingStreamProducer("streamname1", config, kinesis)

    val bytes = mock[ByteBuffer]
    val futureResult = iut.putRecord(bytes, PartitionKey("somepartitionkey"))
    whenReady(futureResult.failed) { result =>
      true
    }

    verify(kinesis, times(11)).putRecord(argThat(PutRecordRequestMatcher("streamname1", bytes, PartitionKey("somepartitionkey"))))
  }

  it should "perform retries until successfully putting record (while within allowed retry limit" in {
    val kinesis = mock[AmazonKinesis]

    val config = mock[KinesisProducerConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(10.milliseconds).when(config).retryBackoffDuration

    doThrow(new AmazonServiceException("testing"))
    .doThrow(new AmazonServiceException("testing"))
    .doReturn(new PutRecordResult().withShardId("testshard1").withSequenceNumber("myseq123"))
      .when(kinesis).putRecord(any[PutRecordRequest])

    val iut = new RetryingStreamProducer("streamname1", config, kinesis)

    val bytes = mock[ByteBuffer]
    val futureResult = iut.putRecord(bytes, PartitionKey("somepartitionkey"))
    whenReady(futureResult) { result =>
      result.isSuccess should be(true)
      result.get should be(PutResult("testshard1", SequenceNumber("myseq123"), 3))
    }

    verify(kinesis, times(3)).putRecord(argThat(PutRecordRequestMatcher("streamname1", bytes, PartitionKey("somepartitionkey"))))
  }
}
