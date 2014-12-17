package com.gilt.gfc.kinesis.consumer

import scala.concurrent.duration._

import com.gilt.gfc.logging.Loggable

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{KinesisClientLibDependencyException, ShutdownException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer

private [consumer] case class RetryingCheckpoint(shardId: String,
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

  override def apply(sequenceNumber: String): Unit = withOnCheckpoint {
    withRetry {
      checkpointer.checkpoint(sequenceNumber)
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
