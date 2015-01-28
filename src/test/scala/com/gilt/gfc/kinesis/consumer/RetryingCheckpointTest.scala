package com.gilt.gfc.kinesis.consumer


import com.gilt.gfc.kinesis.common.{SequenceNumber, ShardId}
import com.gilt.gfc.kinesis.consumer.raw.RetryingCheckpoint

import scala.concurrent.duration._

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException

import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{doThrow, verify, never, times}
import org.mockito.Matchers.anyString

class RetryingCheckpointTest extends FlatSpec with Matchers with MockitoSugar {
  val eldestRecordTS = System.nanoTime.nanoseconds

  "A RetryingCheckpoint" should "delegate calls to checkpoint()" in {
    val checkpointer = mock[IRecordProcessorCheckpointer]

    @volatile var callbackIssued = false
    val iut = RetryingCheckpoint(ShardId("shard"), checkpointer, 3, 10.milliseconds, eldestRecordTS, 10) {
      callbackIssued = true
    }

    iut()

    callbackIssued should be (true)
    verify(checkpointer, times(1)).checkpoint()
    verify(checkpointer, never).checkpoint(anyString)
  }

  it should "delegate calls to checkpoint(sequenceNumber)" in {
    val checkpointer = mock[IRecordProcessorCheckpointer]

    @volatile var callbackIssued = false
    val iut = RetryingCheckpoint(ShardId("shard"), checkpointer, 3, 10.milliseconds, eldestRecordTS, 10) {
      callbackIssued = true
    }

    iut(SequenceNumber("somesequencenumber"))

    callbackIssued should be (true)
    verify(checkpointer, never).checkpoint()
    verify(checkpointer, times(1)).checkpoint("somesequencenumber")
  }

  it should "retry the allowed number of times on continued ClientLib failure" in {
    val checkpointer = mock[IRecordProcessorCheckpointer]

    doThrow(new KinesisClientLibDependencyException("testing")).when(checkpointer).checkpoint()

    @volatile var callbackIssued = false
    val iut = RetryingCheckpoint(ShardId("shard"), checkpointer, 3, 10.milliseconds, eldestRecordTS, 10) {
      callbackIssued = true
    }

    a [KinesisClientLibDependencyException] should be thrownBy {
      iut()
    }

    callbackIssued should be (false)
    verify(checkpointer, times(3)).checkpoint()
    verify(checkpointer, never).checkpoint(anyString)
  }
}
