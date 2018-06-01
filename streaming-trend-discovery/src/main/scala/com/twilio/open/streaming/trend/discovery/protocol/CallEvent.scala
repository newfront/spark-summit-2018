package com.twilio.open.streaming.trend.discovery.protocol

sealed trait Event extends Product with Serializable {
  val name: String
}

// unfortunately scala enum's don't play well with spark expression encoder
object CallState {
  private final val states = Map(
    0 -> "initialized",
    1 -> "answered",
    2 -> "completed",
    3 -> "failed"
  )

  def stateFor(ordinal: Int): Option[String] = {
    states.get(ordinal)
  }

}

@SerialVersionUID(1L)
case class SignalingEvent(override val name: String, pdd: Option[Double], callState: Option[Int])
  extends Event

@SerialVersionUID(1L)
case class CallEvent(eventTime: Long, loggedTime: Long, eventId: String, eventType: String,
                     dimensions: Dimensions, signalingEvent: Option[SignalingEvent]=None)

@SerialVersionUID(1L)
case class Dimensions(
  country: Option[String]=None,
  continent: Option[String]=None,
  carrier: Option[String]=None,
  direction: Option[String]=None)

@SerialVersionUID(1L)
case class Metric(timestamp: java.sql.Timestamp, metricAggregation: Array[Byte], value: Double)
  extends Serializable
