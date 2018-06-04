package com.twilio.open.streaming.trend.discovery

import com.googlecode.protobuf.format.JsonFormat
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import com.twilio.open.protocol.Calls.{CallEventType, CallState, CallStateEvent, Carrier, Country, Direction, PddEvent, SignalingEvent, SignalingEventType, CallEvent => CallEventProto, Dimensions => DimensionsProto}

class ProtocolSpec extends FlatSpec with Matchers with BeforeAndAfter {

  // example using protobuf ser/deser with Message and sub-Message
  "CallEvent with Signaling Event" should " serialize to json " in {

    val signalingEvent = SignalingEvent.newBuilder()
      .setEventType(SignalingEventType.call_state)
      .setName("progress")
      .setCallState(CallStateEvent.newBuilder()
        .setState(CallState.initialized).build())
      .build()

    val ce = CallEventProto.newBuilder()
      .setEventId("CE1234")
      .setEventTime(1527966284972L)
      .setLoggedEventTime(1527966304972L)
      .setEventType(CallEventType.signaling_event)
      .setSignalingEvent(signalingEvent)
      .setEventDimensions(DimensionsProto.newBuilder()
        .setCarrier(Carrier.telco_a)
        .setCountry(Country.us)
        .setDirection(Direction.outbound)
        .build()
      ).build()

    val jsonOutput = new JsonFormat().printToString(ce)
    jsonOutput shouldEqual "{\"event_time\": 1527966284972,\"logged_event_time\": 1527966304972,\"event_id\": \"CE1234\",\"event_type\": \"signaling_event\",\"signaling_event\": {\"name\": \"progress\",\"event_type\": \"call_state\",\"call_state\": {\"state\": \"initialized\"}},\"event_dimensions\": {\"country\": \"us\",\"direction\": \"outbound\",\"carrier\": \"telco_a\"}}"

  }

  "CallEvent with Signaling Event" should " for pdd type and serialize to json " in {

    val signalingEvent = SignalingEvent.newBuilder()
      .setEventType(SignalingEventType.pdd)
      .setName("preflight").setPdd(PddEvent.newBuilder().setPdd(2.8f).build())
      .build()

    val ce = CallEventProto.newBuilder()
      .setEventId("CE1234")
      .setEventTime(1527966284972L)
      .setLoggedEventTime(1527966304972L)
      .setEventType(CallEventType.signaling_event)
      .setSignalingEvent(signalingEvent)
      .setRouteId("RI123")
      .setEventDimensions(DimensionsProto.newBuilder()
        .setCarrier(Carrier.telco_a)
        .setCountry(Country.us)
        .setDirection(Direction.outbound)
        .build()
      ).build()

    val jsonOutput = new JsonFormat().printToString(ce)
    jsonOutput shouldEqual "{\"event_time\": 1527966284972,\"logged_event_time\": 1527966304972,\"event_id\": \"CE1234\",\"route_id\": \"RI123\",\"event_type\": \"signaling_event\",\"signaling_event\": {\"name\": \"preflight\",\"event_type\": \"pdd\",\"pdd\": {\"pdd\": 2.8}},\"event_dimensions\": {\"country\": \"us\",\"direction\": \"outbound\",\"carrier\": \"telco_a\"}}"

  }

}
