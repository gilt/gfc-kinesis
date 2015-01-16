package com.gilt.gfc.kinesis.consumer

import java.util.concurrent.{ExecutorService, Executors}
import java.util.{List => JList}

import com.gilt.gfc.kinesis.common.ShardId

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.gilt.gfc.logging.Loggable

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

/**
 * A Consumer of data from a Kinesis Stream.
 *
 * If multiple shards exist, the processBatch function here will be executed from different threads, possibly concurrently.
 *
 * @param streamName The name of the Kinesis stream to consume
 * @param config The configuration
 * @param executorService to use for processing records - this is passed to, and used by, the underlying Java Amazon library. Defaults to Executors.newCachedThreadPool()
 * @param processBatch A function that is called for each "batch" of records received from the kinesis stream.
 *                     This function may be called from different threads (for different shards), and possibly even concurrently.
 *                     '''Note''' this function should complete in less time than the configured
 *                     [[com.gilt.gfc.kinesis.consumer.KinesisConsumerConfig.leaseFailoverTime]] as otherwise
 *                     the lease could expire while processing is proceeding.
 */
case class RawKinesisStreamConsumer(streamName: String,
                                    config: KinesisConsumerConfig,
                                    executorService: ExecutorService = Executors.newCachedThreadPool())
                                    (processBatch: (Seq[Record], Checkpoint) => Unit) extends Loggable {
  @volatile private var workerOpt: Option[Worker] = None
  @volatile private var onShardShutdownOpt: Option[(Checkpoint, ShardId) => Unit] = None

  /**
   * Register a function to be called when a shard is being terminated.
   *
   * This can be useful to allow applications decide their own checkpoint workflow, rather than checkpointing every "batch".
   *
   * If no function is registered here, then an automatic checkpoint is issued on shard termination.
   *
   * Only a single registration is allowed.
   *
   * @param func A function that will be called on shard termination - this is passed a Checkpoint and the ID of the shard
   *             being terminated.
   */
  def onShardShutdown(func: (Checkpoint, ShardId) => Unit): Unit = {
    onShardShutdownOpt.foreach(_ => sys.error("Only allowed to register a single shardShutdown handler"))
    onShardShutdownOpt = Some(func)
  }

  /**
   * Start reading data from the stream.
   */
  def start(): Unit = {
    workerOpt.foreach(_ => sys.error("Already started..."))
    workerOpt = Some(new Worker(new DelegatingIRecordProcessorFactory(), config.createStreamConfiguration(streamName), executorService))
    workerOpt.foreach { worker =>
      info(s"Starting consuming from $streamName")
      new Thread(worker).start()
    }
  }

  /**
   * Initiate the shutdown of processing the stream.
   */
  def shutdown(): Unit = {
    workerOpt.foreach(_.shutdown())
    workerOpt = None
  }

  /**
   * Internal implementation of the Amazon Kinesis client library
   */
  private [kinesis] class DelegatingIRecordProcessorFactory extends IRecordProcessorFactory {
    override def createProcessor(): IRecordProcessor = new DelegatingIRecordProcessor()
  }

  /**
   * Internal implementation of the Amazon Kinesis client library
   */
  private [kinesis] class DelegatingIRecordProcessor extends IRecordProcessor {
    @volatile private var shardIdOpt: Option[ShardId] = None
    @volatile private var uncheckpointedRecordTimestamp: Option[FiniteDuration] = None
    @volatile private var uncheckpointedCount: Long = 0

    override def initialize(shardId: String): Unit = {
      info(s"initialize($streamName.$shardId)")
      shardIdOpt = Option(ShardId(shardId))
    }

    override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
      import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason._
      reason match {
        case TERMINATE =>
          info(s"shutdown.terminate received for shard '$knownShardId' - issuing checkpoint for termination")
          val checkpoint = makeCheckpoint(checkpointer, uncheckpointedRecordTimestamp.getOrElse(System.nanoTime.nanoseconds), uncheckpointedCount)

          // if application registered shardShutdown, call, otherwise directly checkpoint here.
          onShardShutdownOpt.fold(checkpoint())(_(checkpoint, knownShardId))

          // Force a checkpoint if still required
          if (uncheckpointedCount > 0) {
            warn(s"forcing explicit checkpoint for shard $knownShardId")
            checkpoint()
          }

        case ZOMBIE =>
          info(s"shutdown.zombie received for shard '$knownShardId'")
      }
      shardIdOpt = None
    }

    override def processRecords(jRecords: JList[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
      val records = jRecords.asScala
      /*
       * Should process this batch if any of the following hold true:
       * 1. this batch is not empty
       * 2. we are configured to pass all empty batches regardless
       * 3. the batch is empty and there have been records in batches passed to application since last checkpoint.
       */
      if (records.nonEmpty || config.processAllEmptyBatches || uncheckpointedRecordTimestamp.isDefined) {
        val ts = uncheckpointedRecordTimestamp.getOrElse {
          // No record timestamp present, create a new one for record in this batch.
          val tempTimestamp = System.nanoTime.nanoseconds
          // but only track if there is actually a record in this batch.
          if (records.nonEmpty) {
            uncheckpointedRecordTimestamp = Some(tempTimestamp)
          }
          tempTimestamp
        }
        uncheckpointedCount += jRecords.size
        processBatch(records, makeCheckpoint(checkpointer, ts, uncheckpointedCount))
      }
    }

    private def knownShardId: ShardId = shardIdOpt.getOrElse(ShardId("<unset>"))

    private def onCheckpoint: Unit = {
      uncheckpointedRecordTimestamp = None
      uncheckpointedCount = 0
    }

    private def makeCheckpoint(checkpointer: IRecordProcessorCheckpointer, ts: FiniteDuration, size: Long): Checkpoint = {
      new RetryingCheckpoint(knownShardId, checkpointer, config.checkpointFailRetryCount, config.checkpointFailBackoff, ts, size)(onCheckpoint)
    }
  }
}
