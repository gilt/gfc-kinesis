package com.gilt.gfc.kinesis.publisher

import java.util.concurrent.{Future => JFuture}
import com.amazonaws.AmazonServiceException
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisAsync}
import com.amazonaws.services.kinesis.model.{PutRecordRequest, PutRecordResult}
import com.gilt.gfc.kinesis.common.{SequenceNumber, ShardId}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.Matchers.{any, argThat}
import org.mockito.Mockito.{doReturn, doAnswer, when, doThrow, times, verify}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
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

  private case class SuccessAnswer(shardId: String, sequenceNumber: String) extends Answer[JFuture[PutRecordResult]] {
    override def answer(invocation: InvocationOnMock): JFuture[PutRecordResult] = {
      val args = invocation.getArguments()
      val request = args(0).asInstanceOf[PutRecordRequest]
      val asyncHandler = args(1).asInstanceOf[AsyncHandler[PutRecordRequest, PutRecordResult]]
      asyncHandler.onSuccess(request, new PutRecordResult().withShardId("testshard1").withSequenceNumber("myseq123"))

      mock[JFuture[PutRecordResult]]
    }
  }

  private case class FailAnswer(msg: String) extends Answer[JFuture[PutRecordResult]] {
    override def answer(invocation: InvocationOnMock): JFuture[PutRecordResult] = {
      val args = invocation.getArguments()
      val asyncHandler = args(1).asInstanceOf[AsyncHandler[PutRecordRequest, PutRecordResult]]
      asyncHandler.onError(new Exception(msg))

      mock[JFuture[PutRecordResult]]
    }
  }


  "A RawKinesisStreamProducer" should "successfully put record" in {
    val kinesis = mock[AmazonKinesisAsync]

    val config = mock[KinesisPublisherConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(10.milliseconds).when(config).retryBackoffDuration

    doAnswer(new SuccessAnswer("testshard1", "myseq123"))
      .when(kinesis).putRecordAsync(any[PutRecordRequest], any[AsyncHandler[PutRecordRequest, PutRecordResult]])

    val iut = new RetryingStreamPublisher("streamname1", config, kinesis)

    val rawRecord = RawRecord("hi earth".getBytes, PartitionKey("some partition key"))

    val futureResult = iut.putRecord(rawRecord)

    whenReady(futureResult) { result =>
      result.isSuccess should be(true)
      result.get should be(PutResult(ShardId("testshard1"), SequenceNumber("myseq123"), 1))
    }

    verify(kinesis, times(1)).putRecordAsync(any[PutRecordRequest], any[AsyncHandler[PutRecordRequest, PutRecordResult]])
  }

  it should "exercise all allowed retries on continual failure" in {
    val kinesis = mock[AmazonKinesisAsync]

    val config = mock[KinesisPublisherConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(5.milliseconds).when(config).retryBackoffDuration

    doThrow(new AmazonServiceException("testing")).when(kinesis).putRecordAsync(any[PutRecordRequest], any[AsyncHandler[PutRecordRequest, PutRecordResult]])

    val iut = new RetryingStreamPublisher("streamname1", config, kinesis)

    val bytes = "dia dhuit an domhain".getBytes
    val futureResult = iut.putRecord(RawRecord(bytes, PartitionKey("somepartitionkey")))
    whenReady(futureResult.failed) { result =>
      true
    }

    verify(kinesis, times(11)).putRecordAsync(argThat(PutRecordRequestMatcher("streamname1", bytes, PartitionKey("somepartitionkey"))), any[AsyncHandler[PutRecordRequest, PutRecordResult]])
  }

  it should "perform retries until successfully putting record (while within allowed retry limit" in {
    val kinesis = mock[AmazonKinesisAsync]

    val config = mock[KinesisPublisherConfig]

    doReturn(10).when(config).allowedRetriesOnFailure
    doReturn(5.milliseconds).when(config).retryBackoffDuration

    doAnswer(new FailAnswer("testing"))
    .doAnswer(new FailAnswer("testing"))
    .doAnswer(new SuccessAnswer("testshard1", "myseq123"))
      .when(kinesis).putRecordAsync(any[PutRecordRequest], any[AsyncHandler[PutRecordRequest, PutRecordResult]])

    val iut = new RetryingStreamPublisher("streamname1", config, kinesis)

    val bytes = "hello world".getBytes
    val futureResult = iut.putRecord(RawRecord(bytes, PartitionKey("somepartitionkey")))
    whenReady(futureResult) { result =>
      result.isSuccess should be(true)
      result.get should be(PutResult(ShardId("testshard1"), SequenceNumber("myseq123"), 3))
    }

    verify(kinesis, times(3)).putRecordAsync(argThat(PutRecordRequestMatcher("streamname1", bytes, PartitionKey("somepartitionkey"))), any[AsyncHandler[PutRecordRequest, PutRecordResult]])
  }
}
