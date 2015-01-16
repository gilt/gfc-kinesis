package com.gilt.gfc.kinesis.consumer

import com.gilt.gfc.kinesis.common.ShardId

import scala.concurrent.duration._

import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.model.Record
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{doReturn, doAnswer, verify, times}
import org.mockito.Matchers.{any, eq => mockEq, argThat}
import org.hamcrest.{Description, BaseMatcher}

class RawKinesisStreamConsumerTest extends FlatSpec with Matchers with MockitoSugar {
  private class CheckpointSizeMatcher(targetSize: Long) extends BaseMatcher[Checkpoint] {
    override def matches(p1: Any): Boolean = {
      val actualSize = p1.asInstanceOf[Checkpoint].size()
      targetSize == actualSize
    }
    override def describeTo(desc: Description): Unit = desc.appendText(s"Checkpoint with size of $targetSize")
  }

  "A RawKinesisStreamConsumer" should "pass batch of records to the processBatch function" in {
    val config = mock[KinesisConsumerConfig]
    val record1 = mock[Record]
    val record2 = mock[Record]
    val record3 = mock[Record]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)

    val factory = new iut.DelegatingIRecordProcessorFactory()

    val processor = factory.createProcessor()

    processor.initialize("testshard")
    processor.processRecords(Seq(record1, record2, record3).asJava, kinesisCheckpointer)

    verify(processBatch, times(1)).apply(mockEq(Seq(record1, record2, record3)), argThat(new CheckpointSizeMatcher(3)))
  }

  it should "pass batch of records to the processBatch function with increasing checkpoint age (in the absense of checkpointing)" in {
    val config = mock[KinesisConsumerConfig]
    val record1 = mock[Record]
    val record2 = mock[Record]
    val record3 = mock[Record]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]

    class CheckpointValidatingAnswer extends Answer[Unit] {
      @volatile var lastAge: FiniteDuration = 0.nanoseconds

      override def answer(invocation: InvocationOnMock): Unit = {
        val checkpoint = invocation.getArguments()(1).asInstanceOf[Checkpoint]
        checkpoint.age should be > lastAge
        lastAge = checkpoint.age
      }
    }

    // Track the continuing age across several batches
    val checkpointValidatingAnswer = new CheckpointValidatingAnswer()
    doAnswer(checkpointValidatingAnswer).when(processBatch).apply(any[Seq[Record]], any[Checkpoint])

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)

    val factory = new iut.DelegatingIRecordProcessorFactory()
    val processor = factory.createProcessor()
    processor.initialize("testshard")

    processor.processRecords(Seq(record1, record2, record3).asJava, kinesisCheckpointer)
    Thread.sleep(1.second.toMillis)
    processor.processRecords(Seq(record1, record2, record3).asJava, kinesisCheckpointer)
    Thread.sleep(1.second.toMillis)
    processor.processRecords(Seq(record1, record2, record3).asJava, kinesisCheckpointer)

    verify(processBatch, times(3)).apply(any[Seq[Record]], any[Checkpoint])
  }

  "A KinesisStreamConsumer configured not to receive ALL empty batches" should "pass empty batchs to the processBatch function while there are uncheckpointed records" in {
    val config = mock[KinesisConsumerConfig]
    doReturn(false).when(config).processAllEmptyBatches
    val record1 = mock[Record]
    val record2 = mock[Record]
    val record3 = mock[Record]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)

    val factory = new iut.DelegatingIRecordProcessorFactory()

    val processor = factory.createProcessor()

    processor.initialize("testshard")

    // Pass some batchs with records...
    processor.processRecords(Seq(record1).asJava, kinesisCheckpointer)
    processor.processRecords(Seq(record2, record3).asJava, kinesisCheckpointer)
    // Now pass 2 empty batches...
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)

    verify(processBatch, times(1)).apply(mockEq(Seq(record1)), argThat(new CheckpointSizeMatcher(1)))
    verify(processBatch, times(1)).apply(mockEq(Seq(record2, record3)), argThat(new CheckpointSizeMatcher(3)))
    verify(processBatch, times(2)).apply(mockEq(Nil), argThat(new CheckpointSizeMatcher(3)))
  }

  it should "pass empty batchs to the processBatch function until a checkpoint is issued" in {
    val config = mock[KinesisConsumerConfig]
    doReturn(false).when(config).processAllEmptyBatches
    val record1 = mock[Record]
    val record2 = mock[Record]
    val record3 = mock[Record]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]

    @volatile var processCount = 0

    doAnswer(new Answer[Unit]{
      override def answer(invocation: InvocationOnMock): Unit = {
        processCount += 1
        // Checkpoint on the second call..
        if (processCount == 2) {
          val checkpoint = invocation.getArguments()(1).asInstanceOf[Checkpoint]
          checkpoint()
        }
      }
    }).when(processBatch).apply(any[Seq[Record]], any[Checkpoint])

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)

    val factory = new iut.DelegatingIRecordProcessorFactory()

    val processor = factory.createProcessor()

    processor.initialize("testshard")

    // Pass a single batch with records...
    processor.processRecords(Seq(record1, record2, record3).asJava, kinesisCheckpointer)
    // Now pass several empty batches...
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)

    verify(processBatch, times(1)).apply(mockEq(Seq(record1, record2, record3)), any[Checkpoint])
    verify(processBatch, times(1)).apply(mockEq(Nil), any[Checkpoint])
  }

  it should "call onShutdown when a shard terminates" in {
    val config = mock[KinesisConsumerConfig]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]
    val onShutdown = mock[Function2[Checkpoint, ShardId, Unit]]

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)
    iut.onShardShutdown(onShutdown)

    val factory = new iut.DelegatingIRecordProcessorFactory()
    val processor = factory.createProcessor()
    processor.initialize("testshard")

    processor.shutdown(kinesisCheckpointer, ShutdownReason.TERMINATE)

    verify(onShutdown).apply(argThat(new CheckpointSizeMatcher(0)), mockEq(ShardId("testshard")))
  }

  it should "call onShutdown when a shard terminates, passing the correct count of uncheckpointed records in the Checkpoint" in {
    val config = mock[KinesisConsumerConfig]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]
    val onShutdown = mock[Function2[Checkpoint, ShardId, Unit]]

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)
    iut.onShardShutdown(onShutdown)

    val factory = new iut.DelegatingIRecordProcessorFactory()
    val processor = factory.createProcessor()
    processor.initialize("testshard")

    processor.processRecords(Seq(mock[Record]).asJava, kinesisCheckpointer)
    processor.processRecords(Seq(mock[Record]).asJava, kinesisCheckpointer)

    processor.shutdown(kinesisCheckpointer, ShutdownReason.TERMINATE)

    verify(onShutdown).apply(argThat(new CheckpointSizeMatcher(2)), mockEq(ShardId("testshard")))
  }

  "A RawKinesisStreamConsumer configured to receive ALL empty batches" should "pass all empty batchs to the processBatch function" in {
    val config = mock[KinesisConsumerConfig]
    doReturn(true).when(config).processAllEmptyBatches
    val record1 = mock[Record]
    val record2 = mock[Record]
    val record3 = mock[Record]
    val processBatch = mock[Function2[Seq[Record], Checkpoint, Unit]]
    val kinesisCheckpointer = mock[IRecordProcessorCheckpointer]

    @volatile var processCount = 0

    doAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        processCount += 1
        // Checkpoint on the first call...
        if (processCount == 1) {
          val checkpoint = invocation.getArguments()(1).asInstanceOf[Checkpoint]
          checkpoint()
        }
      }
    }).when(processBatch).apply(any[Seq[Record]], any[Checkpoint])

    val iut = new RawKinesisStreamConsumer("teststream", config)(processBatch)

    val factory = new iut.DelegatingIRecordProcessorFactory()
    val processor = factory.createProcessor()
    processor.initialize("testshard")

    // Pass a single batch with records...
    processor.processRecords(Seq(record1, record2, record3).asJava, kinesisCheckpointer) // Should checkpoint on this one..
    // Now pass several empty batches...
    processor.processRecords(Seq.empty[Record].asJava, kinesisCheckpointer)

    verify(processBatch, times(1)).apply(mockEq(Seq(record1, record2, record3)), argThat(new CheckpointSizeMatcher(3)))
    verify(processBatch, times(1)).apply(mockEq(Nil), argThat(new CheckpointSizeMatcher(0)))
  }
}
