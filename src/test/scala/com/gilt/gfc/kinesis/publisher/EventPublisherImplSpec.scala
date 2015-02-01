package com.gilt.gfc.kinesis.publisher

import com.gilt.gfc.kinesis.common.SequenceNumber
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.mockito.Matchers.{any, eq => mockEq}
import org.mockito.Mockito.{doReturn, doThrow, never, verify, inOrder => mockInOrder}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class EventPublisherImplSpec extends FlatSpec with Matchers with MockitoSugar with ScalaFutures {

  "A publisher" should "put a correctly converted record when publish is called" in {
    val rawPublisher = mock[RawKinesisStreamPublisher]
    val rawRecord1 = mock[RawRecord]
    val putResult1 = mock[PutResult]

    val convertFunction = mock[String => RawRecord]

    doReturn(rawRecord1).when(convertFunction).apply("hello")

    doReturn(Future.successful(Success(putResult1))).when(rawPublisher).putRecord(mockEq(rawRecord1), any[Option[SequenceNumber]])

    val publisher = new EventPublisherImpl[String](rawPublisher, convertFunction)

    whenReady(publisher.publish("hello")) { result =>
      result should be (Success(()))
    }

    verify(rawPublisher).putRecord(rawRecord1)
  }

  it should "correctly propogate a failure to publish" in {
    val rawPublisher = mock[RawKinesisStreamPublisher]
    val rawRecord1 = mock[RawRecord]
    val putResult1 = mock[PutResult]

    val convertFunction = mock[String => RawRecord]

    doReturn(rawRecord1).when(convertFunction).apply("hello")

    doReturn(Future.successful(Failure(new Exception("only testing")))).when(rawPublisher).putRecord(mockEq(rawRecord1), any[Option[SequenceNumber]])

    val publisher = new EventPublisherImpl[String](rawPublisher, convertFunction)

    whenReady(publisher.publish("hello")) { result =>
      result.isFailure should be (true)
    }

    verify(rawPublisher).putRecord(rawRecord1)
  }

  it should "correctly publish a sequence of events serially" in {
    val rawPublisher = mock[RawKinesisStreamPublisher]
    val rawRecord1 = mock[RawRecord]
    val rawRecord2 = mock[RawRecord]
    val rawRecord3 = mock[RawRecord]

    val convertFunction = mock[String => RawRecord]

    doReturn(rawRecord1).when(convertFunction).apply("one")
    doReturn(rawRecord2).when(convertFunction).apply("two")
    doReturn(rawRecord3).when(convertFunction).apply("three")

    doReturn(Future.successful(Success(mock[PutResult]))).when(rawPublisher).putRecord(any[RawRecord], any[Option[SequenceNumber]])

    val publisher = new EventPublisherImpl[String](rawPublisher, convertFunction)

    whenReady(publisher.publishSequentially(Seq("one", "two", "three"))) { result =>
      result should be (3)
    }

    val ino = mockInOrder(rawPublisher)
    ino.verify(rawPublisher).putRecord(rawRecord1)
    ino.verify(rawPublisher).putRecord(rawRecord2)
    ino.verify(rawPublisher).putRecord(rawRecord3)
  }

  it should "stop publishing a sequence of events after the first failure" in {
    val rawPublisher = mock[RawKinesisStreamPublisher]
    val rawRecord1 = mock[RawRecord]
    val rawRecord2 = mock[RawRecord]
    val rawRecord3 = mock[RawRecord]

    val convertFunction = mock[String => RawRecord]

    doReturn(rawRecord1).when(convertFunction).apply("one")
    doReturn(rawRecord2).when(convertFunction).apply("two")
    doReturn(rawRecord3).when(convertFunction).apply("three")

    doReturn(Future.successful(Success(mock[PutResult]))).when(rawPublisher).putRecord(any[RawRecord], any[Option[SequenceNumber]])
    doReturn(Future.successful(Failure(new Exception("only testing")))).when(rawPublisher).putRecord(mockEq(rawRecord2), any[Option[SequenceNumber]])

    val publisher = new EventPublisherImpl[String](rawPublisher, convertFunction)

    whenReady(publisher.publishSequentially(Seq("one", "two", "three"))) { result =>
      result should be (1)
    }

    val ino = mockInOrder(rawPublisher)
    ino.verify(rawPublisher).putRecord(rawRecord1)
    ino.verify(rawPublisher).putRecord(rawRecord2)
    ino.verify(rawPublisher, never).putRecord(rawRecord3)

    verify(convertFunction, never).apply("three")
  }
}
