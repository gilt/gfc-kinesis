package com.gilt.gfc.kinesis.producer

import com.gilt.gfc.kinesis.common.{ShardId, SequenceNumber}

case class PutResult(shardId: ShardId, sequenceNumber: SequenceNumber, attemptCount: Int)
