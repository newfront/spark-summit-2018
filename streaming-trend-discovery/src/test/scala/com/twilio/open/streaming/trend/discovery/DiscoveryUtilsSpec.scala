package com.twilio.open.streaming.trend.discovery

import java.io._
import java.time.Instant
import java.time.format.DateTimeFormatter

import com.twilio.open.streaming.trend.discovery.protocol.{CallEvent, Dimensions}
import org.scalatest.{FlatSpec, Matchers}

trait SerializableProduct extends Product with Serializable
case class SimpleClass(name:String, age:Int) extends SerializableProduct

class DiscoveryUtilsSpec extends FlatSpec with Matchers {

  // example using java serialization with case class
  "DiscoveryUtils" should " serialize and deserialize a CallEvent object" in {
    val eventTime = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse("2018-03-08T18:00:00Z"))
    val loggedTime = eventTime.plusSeconds(34)
    //eventTime: Long, loggedTime: Long, eventId: String, eventType: String,dimensions: Dimensions, signalingEvent: Option[SignalingEvent]
    //case class Dimensions(country: Option[String], continent: Option[String], carrier: Option[String],direction: Option[String])
    val ce = CallEvent(eventTime.toEpochMilli, loggedTime.toEpochMilli, "uuid1", "signaling", Dimensions(
      country = Some("us"),
      continent = Some("na"),
      carrier = Some("verizon"),
      direction = Some("inbound")
    ), None)

    val ceSer = DiscoveryUtils.serialize(ce)
    val ceDeser = DiscoveryUtils.deserialize[CallEvent](ceSer)
    ce.equals(ceDeser)
  }

  // example using protobuf ser/deser with Message and sub-Message

}
