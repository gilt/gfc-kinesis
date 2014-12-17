gfc-kinesis
===========

A library providing simple Scala API for Amazon Kinesis. Part of the gilt foundation classes

## Stream Consumption

Package `com.gilt.gfc.kinesis.consumer` provides a lightweight wrapper for Amazon's Java client library (`IRecordProcessorFactory`, etc.) is provided - this includes using DynamoDB for lease/checkpoint coordination of streams (and shards thereof).

    val config = new KinesisClientConfiguration {
        override def appName = "example-app-name"
    }

    val consumer = KinesisStreamConsumer("example-kinesis-stream-name", config) { 
        (records, checkpoint) =>
            records.foreach { record =>
               // Process the Kinesis Record
               println(new String(record.getData.array))
            }

            // Simple periodic checkpointing of each shard
            if (checkpoint.age > 3.minutes) checkpoint()
    }

    consumer.start()


## License
Copyright 2014 Gilt Groupe, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
