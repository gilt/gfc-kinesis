package com.gilt.gfc.kinesis.producer.raw

import com.gilt.gfc.kinesis.common.{SequenceNumber, ShardId}

case class PutResult(shardId: ShardId, sequenceNumber: SequenceNumber, attemptCount: Int)
