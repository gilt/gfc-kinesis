package com.gilt.gfc.kinesis.consumer.raw

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{KinesisClientLibDependencyException, ShutdownException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.gilt.gfc.kinesis.common.{SequenceNumber, ShardId}
import com.gilt.gfc.logging.Loggable

import scala.concurrent.duration._

private [consumer] case class RetryingCheckpoint(shardId: ShardId,
                                                 checkpointer: IRecordProcessorCheckpointer,
                                                 retryCount: Int,
                                                 backoff: FiniteDuration,
                                                 eldestRecordTS: FiniteDuration,
                                                 override val size: Long)
                                                 (onCheckpoint: => Unit) extends Checkpoint with Loggable {

  override def age(): FiniteDuration = System.nanoTime.nanoseconds - eldestRecordTS

  override def apply(): Unit = withOnCheckpoint {
    withRetry {
      checkpointer.checkpoint()
    }
  }

  override def apply(sequenceNumber: SequenceNumber): Unit = withOnCheckpoint {
    withRetry {
      checkpointer.checkpoint(sequenceNumber.value)
    }
  }

  private def withOnCheckpoint(checkpoint: => Unit): Unit = {
    checkpoint
    onCheckpoint
  }

  private def withRetry(checkpoint: => Unit): Unit = {
    def recurs(remainingAttempts: Int): Unit = {
      try {
        checkpoint
      } catch {
        case se: ShutdownException =>
          warn(s"processor for shard $shardId already shutdown - stopping processing records here")
          throw se // Propagate to complete handling

        case ex: KinesisClientLibDependencyException if remainingAttempts > 1 =>
          warn(s"Failed to checkpoint $shardId - issue storing the checkpoint - backing off, and retrying")
          Thread.sleep(backoff.toMillis)
          recurs(remainingAttempts - 1)
      }
    }
    recurs(retryCount)
  }
}
