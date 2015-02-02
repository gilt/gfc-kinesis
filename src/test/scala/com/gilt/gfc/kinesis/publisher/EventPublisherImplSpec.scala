package com.gilt.gfc.kinesis.publisher

import com.gilt.gfc.kinesis.common.{ShardId, SequenceNumber}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.mockito.Matchers.{any, eq => mockEq}
import org.mockito.Mockito.{doReturn, doThrow, never, verify, inOrder => mockInOrder}

import scala.concurrent.Future
import scala.util.{Try, Failure, Success}

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
    val rawRecord1 = RawRecord("one".getBytes, PartitionKey("key1"))
    val rawRecord2 = RawRecord("two".getBytes, PartitionKey("key2"))
    val rawRecord3 = RawRecord("three".getBytes, PartitionKey("key3"))

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
    ino.verify(rawPublisher).putRecord(rawRecord1, None) // All of different partition-keys... 
    ino.verify(rawPublisher).putRecord(rawRecord2, None)
    ino.verify(rawPublisher).putRecord(rawRecord3, None)
  }

  it should "stop publishing a sequence of events after the first failure" in {
    object RawPublisher extends RawKinesisStreamPublisher {
      val placed = scala.collection.mutable.ArrayBuffer[(RawRecord, Option[SequenceNumber])]()

      var putCount = 0 // Hardcoded to fail on second put attempt!
      override def putRecord(record: RawRecord, sequenceNumberForOrdering: Option[SequenceNumber]): Future[Try[PutResult]] = {
        putCount += 1
        placed += ((record, sequenceNumberForOrdering))

        Future.successful {
          if (putCount < 2) Success(PutResult(ShardId("shard"), SequenceNumber(s"seq-$putCount"), 1))
          else Failure(new Exception("only testing"))
        }
      }

      override def shutdown(): Unit = throw new Exception("Unexpected call...")
    }

    val rawRecord1 = RawRecord("one".getBytes, PartitionKey("pkey"))
    val rawRecord2 = RawRecord("two".getBytes, PartitionKey("pkey"))
    val rawRecord3 = RawRecord("three".getBytes, PartitionKey("pkey"))

    val convertFunction = mock[String => RawRecord]

    doReturn(rawRecord1).when(convertFunction).apply("one")
    doReturn(rawRecord2).when(convertFunction).apply("two")
    doReturn(rawRecord3).when(convertFunction).apply("three")

    val sequenceNumber1 = SequenceNumber("snum1")
    val sequenceNumber2 = SequenceNumber("snum2")
    val sequenceNumber3 = SequenceNumber("snum3")

    def putResult(sNumber: SequenceNumber) = PutResult(ShardId("someshard"), sNumber, 1)

    val publisher = new EventPublisherImpl[String](RawPublisher, convertFunction)

    whenReady(publisher.publishSequentially(Seq("one", "two", "three"))) { result =>
      result should be (1)
    }

    verify(convertFunction, never).apply("three")
    RawPublisher.placed should be (Seq((rawRecord1, None), (rawRecord2, Some(SequenceNumber("seq-1")))))
  }
}
