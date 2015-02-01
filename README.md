gfc-kinesis
===========

A library providing simple Scala API for Amazon Kinesis, for event publishers and consumers. This library is part of the gilt foundation classes.

## Simple Configuration

This library allows simple configuration for both consumers and publishers. The configurations come with readonable defaults (where possible).

## Typed API

This library provides a typed API that allows applications to be written in terms of _typed events_.

The entry point to this API is the `com.gilt.gfc.kinesis.KinesisFactory`. This can be used to create a Publisher:

    val config = new KinesisPublisherConfig {
        override val regionName = "us-west-1"
    }

    def convertString(str: String) = RawRecord(str.getBytes, PartitionKey("sensible-partition-key"))

    val publisher = KinesisFactory.newPublisher("my-stream-name", config, convertString)

    // Now publish some events...
    publisher.publish("event1")
    publisher.publish("event2")

or an event Receiver, which in turn can be used to consume events:

    val config = new KinesisConsumerConfig {
        override val appName = "example-app-name"
        override val regionName = "us-west-1"
    }

    def convert(bytes: Array[Byte]): String = new String(bytes)

    val receiver = KinesisFactory.newReceiver("my-stream-name", config, convert)

    def onEvent(event: String) = println(s"Received event: $event")

    receiver.registerConsumer(onEvent)

    receiver.start()

By default, the Kinesis receiver here checkpoints the stream/shard every 1 minute. This can be changed by specifying a different `com.gilt.gfc.kinesis.consumer.CheckpointingStrategy` which allows for various strategies, including age, event throughput, per-batch, or various other combinations.

## Closer to Kinesis

In addition to the typed API, this library also provides a lower level _raw API_, for both consumer (`com.gilt.gfc.kinesis.consumer.RawKinesisStreamConsumer`) and publisher (`com.gilt.gfc.kinesis.publisher.RawKinesisStreamPublisher`).

This raw API allow for more detailed integration with Kinesis, which may be useful if the event based API is too abstract for certain specific use cases.

The Typed API is built on top of this raw API.


## Raw Stream Publisher

Package `com.gilt.gfc.kinesis.publisher` provides a lightweight wrapper for producing events into a kinesis stream.


    val config = new KinesisPublisherConfig {
        override val regionName = "us-west-1"
    }

    val producer = RawKinesisStreamPublisher("my-stream-name", config)

    val futureResult = producer.putRecord(ByteBuffer.wrap("hello world".getBytes), )
                                          PartitionKey("some partition key value"))

    // Simple example only
    val result = Await.result(futureResult, Duration.Inf)

## Raw Stream Consumer

Package `com.gilt.gfc.kinesis.consumer` provides a lightweight wrapper for Amazon's Java client library (`IRecordProcessorFactory`, etc.) - this includes using DynamoDB for lease/checkpoint coordination of streams (and shards thereof).

    val config = new KinesisConsumerConfig {
        override val appName = "example-app-name"
        override val regionName = "us-west-1"
    }

    val consumer = RawKinesisStreamConsumer("my-stream-name", config) {
        (recordBatch, checkpoint) =>
            recordBatch.foreach { record =>
               println(new String(record.getData.array))
            }

            if (checkpoint.age > 1.minute) checkpoint()
    }

    consumer.start()


## License

Copyright 2015 Gilt Groupe, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
