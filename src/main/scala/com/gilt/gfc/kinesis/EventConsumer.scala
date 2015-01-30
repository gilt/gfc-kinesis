package com.gilt.gfc.kinesis

/**
 * Signature trait for consuming events of a given type.
 * See [[EventReceiver.registerConsumer()]] and [[EventReceiver.unregisterConsumer()]]
 *
 * @tparam T
 */
trait EventConsumer[T] {
  def onEvent(t: T): Unit
}

