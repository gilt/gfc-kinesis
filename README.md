gfc-kinesis
===========

A library providing simple Scala API for Amazon Kinesis. Part of the gilt foundation classes

## Raw Stream Consumer

Package `com.gilt.gfc.kinesis.consumer` provides a lightweight wrapper for Amazon's Java client library (`IRecordProcessorFactory`, etc.) - this includes using DynamoDB for lease/checkpoint coordination of streams (and shards thereof).

    val config = new KinesisConsumerConfig {
        override val appName = "example-app-name"
    }

    val consumer = RawKinesisStreamConsumer("example-kinesis-stream-name", config) {
        (recordBatch, checkpoint) =>
            recordBatch.foreach { record =>
               // Process the Kinesis Record
               println(new String(record.getData.array))
            }

            // Simple periodic checkpointing of each shard
            if (checkpoint.age > 3.minutes) checkpoint()
    }

    consumer.start()


## Raw Stream Producer

Package `com.gilt.gfc.kinesis.producer` provides a lightweight wrapper for producing events into a kinesis stream.

    val config = new KinesisProducerConfig {
        override val regionName = "us-east-1"
    }

    val producer = RawKinesisStreamProducer("example-kinesis-stream-name", config)

    val futureResult = producer.putRecord(ByteBuffer.wrap("hello world".getBytes), )
                                          PartitionKey("some partition key value"))

    // Simple example only
    val result = Await.result(futureResult, Duration.Inf)

## License

Copyright 2014-2015 Gilt Groupe, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
