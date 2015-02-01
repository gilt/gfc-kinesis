package com.gilt.gfc.kinesis.consumer

import com.gilt.gfc.kinesis.consumer.EventReceiver

/**
 * Signature trait for consuming events of a given type.
 * See [[EventReceiver.registerConsumer()]] and [[EventReceiver.unregisterConsumer()]]
 *
 * @tparam T
 */
trait EventConsumer[T] {
  def onEvent(t: T): Unit
}

