package com.twilio.open.streaming.trend.discovery.protocol

import com.google.protobuf.Message
import com.twilio.open.protocol.{Calls, Metrics}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{BinaryType, ObjectType, StructType, TimestampType}

import scala.reflect.{ClassTag, classTag}

object SparkSqlProtobufEncoders {

  def registry[T <: Message : ClassTag]: ProtobufSerde[T] = {
    val MetricAggregationClass = classOf[Metrics.MetricAggregation]


    val serde = classTag[T].runtimeClass match {
      case MetricAggregationClass => MetricAggregationSerde
    }
    serde.asInstanceOf[ProtobufSerde[T]]
  }

  def protobuf[T <: Message : ClassTag]: ExpressionEncoder[T] = {
    val tag = classTag[T]
    val serializer = Seq(serializerFor(BoundReference(0, ObjectType(tag.runtimeClass), nullable = true)))
    val deserializer = deserializerFor(BoundReference(0, BinaryType, nullable = true))

    new ExpressionEncoder[T](
      schema = new StructType().add("value", BinaryType),
      flat = true,
      serializer,
      deserializer,
      clsTag = tag
    )
  }

  def protobufWithTimestamp[T <: Message : ClassTag]: ExpressionEncoder[T] = {
    val tag = classTag[T]
    val serializer = Seq(
      serializerFor(BoundReference(0, ObjectType(tag.runtimeClass), nullable = true)),
      timestampExtractorFor(BoundReference(0, ObjectType(tag.runtimeClass), nullable = true))
    )
    val deserializer = deserializerFor(BoundReference(0, BinaryType, nullable = true))

    new ExpressionEncoder[T](
      schema = new StructType()
        .add("value", BinaryType)
        .add("timestamp", TimestampType),
      flat = false,
      serializer,
      deserializer,
      clsTag = tag
    )
  }

  private def timestampExtractorFor[T <: Message : ClassTag](input: Expression): Expression = {
    val serdeClazz = registry.getClass
    StaticInvoke(serdeClazz, TimestampType, "extractTimestamp", input :: Nil)
  }

  private def serializerFor[T <: Message : ClassTag](input: Expression): Expression = {
    val serdeClazz = registry.getClass
    StaticInvoke(serdeClazz, BinaryType, "serialize", input :: Nil)
  }

  private def deserializerFor[T <: Message : ClassTag](input: Expression): Expression = {
    val serdeClazz = registry.getClass
    val clazz = classTag[T].runtimeClass
    StaticInvoke(serdeClazz, ObjectType(clazz), "deserialize", input :: Nil)
  }

}

sealed trait ProtobufSerde[T <: Message] {
  /** Convert message to bytes */
  def serialize(message: T): Array[Byte]
  /** Deserialize bytes to a message */
  def deserialize(bytes: Array[Byte]): T
  /** Extract time in MICROSECONDS (not milliseconds nor seconds) */
  def extractTimestamp(message: T): Long
}

case object MetricAggregationSerde extends ProtobufSerde[Metrics.MetricAggregation] {
  override def serialize(message: Metrics.MetricAggregation): Array[Byte] = message.toByteArray

  /** Deserialize bytes to a message */
  override def deserialize(bytes: Array[Byte]): Metrics.MetricAggregation = Metrics.MetricAggregation.parseFrom(bytes)

  /** Extract time in MICROSECONDS (not milliseconds nor seconds) */
  override def extractTimestamp(message: Metrics.MetricAggregation): Long = message.getWindow.getStartMs * 1000
}

case object CallEventSerde extends ProtobufSerde[Calls.CallEvent] {
  /** Convert message to bytes */
  override def serialize(message: Calls.CallEvent): Array[Byte] = message.toByteArray

  /** Deserialize bytes to a message */
  override def deserialize(bytes: Array[Byte]): Calls.CallEvent = Calls.CallEvent.parseFrom(bytes)

  /** Extract time in MICROSECONDS (not milliseconds nor seconds) */
  override def extractTimestamp(message: Calls.CallEvent): Long = {
    message.getLoggedEventTime * 1000
  }
}
