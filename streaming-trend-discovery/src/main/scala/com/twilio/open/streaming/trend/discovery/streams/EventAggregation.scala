package com.twilio.open.streaming.trend.discovery.streams

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant

import com.google.protobuf.Message
import com.twilio.open.protocol.Calls._
import com.twilio.open.protocol.Metrics
import com.twilio.open.protocol.Metrics._
import com.twilio.open.streaming.trend.discovery.DiscoveryUtils
import com.twilio.open.streaming.trend.discovery.config.AppConfiguration
import com.twilio.open.streaming.trend.discovery.protocol.{ProtobufEncoderUtils, Metric => ProductMetric}
import com.twilio.open.streaming.trend.discovery.sketches.DoubleSketch
import com.yahoo.memory.Memory
import com.yahoo.sketches.quantiles.DoublesSketch
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.streaming.{Duration, Durations}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object EventAggregation {
  val logger: Logger = LoggerFactory.getLogger(classOf[EventAggregation])

  object implicits {
    lazy val metricEncoder: Encoder[ProductMetric] = Encoders.product[ProductMetric]
    lazy val aggreationEncoder = ProtobufEncoderUtils.protobufMessageEncoder[Metrics.MetricAggregation]
    implicit def longToTimestamp(epochMillis: Long): Timestamp = Timestamp.from(Instant.ofEpochMilli(epochMillis))
  }

  object transforms {
    import DiscoveryUtils._

    def bytesToMetricRow(bytes: Array[Byte], windowInterval: Duration): Option[ProductMetric] = {
      Try(CallEvent.parseFrom(bytes)) match {
        case Success(event) =>
          asMetric(event, windowInterval)
        case Failure(ex) =>
          logger.error(s"issue=failed.to.parse message=Calls.CallEvent error=${ex.getCause.getMessage}")
          None
      }
    }

    def asMetric(callEvent: CallEvent, windowInterval: Duration): Option[ProductMetric] = {
      import implicits.longToTimestamp

      callEvent.getEventType match {
        case CallEventType.signaling_event =>
          val signalingEvent = callEvent.getSignalingEvent
          signalingEvent.getEventType match {
            case SignalingEventType.pdd =>
              val metricName = signalingEvent.getEventType.name()
              val dimensionalHash = generateId(callEvent.getEventDimensions.toByteArray)
              //logger.info(s"dimensionalHash=$dimensionalHash")
              val window = eventWindow(callEvent.getEventTime, windowInterval)
              val metricAggregation = MetricAggregation.newBuilder()
                .setMetric(metricName)
                .setWindowStart(window.getStartMs)
                .setWindowEnd(window.getEndMs)
                .setWindowInterval(window.getInterval)
                .setDimensions(callEvent.getEventDimensions)
                .setDimensionHash(dimensionalHash)
                .build()
              val value: Double = signalingEvent.getPdd.getPdd.toDouble
              val groupingKey = metricAggregation.toByteArray
              Some(ProductMetric(window.getStartMs, groupingKey, value))
            case SignalingEventType.call_state =>
              logger.warn(s"issue=unsupported message=SignalingEventType.call_state error=none")
              None
            case SignalingEventType.unknown_event_type =>
              logger.warn(s"issue=unknown message=SignalingEventType.call_state error=lost.data")
              None
          }
        case CallEventType.unknown_call_event =>
          logger.error(s"issue=unknown.call.event.type message=Calls.CallEventType error=lost.data")
          None
      }
    }

    def setIfPresent[T<: Message.Builder, U](predicate: Boolean, value: U, fun: U => Unit): Unit = {
      if (predicate) fun(value)
    }

    def groupingKey(eventGroup: String, eventName: String, dimensonalHash: String): String = {
      // prefix acts as part of a strong hash
      val groupingKeyPrefix = generateId(
        s"$eventGroup.$eventName.".getBytes(StandardCharsets.UTF_8))
      generateId(
        groupingKeyPrefix.getBytes(StandardCharsets.UTF_8) ++
          dimensonalHash.getBytes(StandardCharsets.UTF_8))
    }
  }

}

@SerialVersionUID(1L)
case class EventAggregation(config: AppConfiguration) extends Serializable {
  import EventAggregation.logger
  private[this] val windowInterval: Duration = Durations.minutes(config.windowIntervalMinutes)
  private[this] val k: Int = 512 // how many k points to store in DoublesSketch

  lazy val outputMode: OutputMode = {
    config.outputMode match {
      case "complete" =>
        OutputMode.Complete()
      case "update" =>
        OutputMode.Update()
      case "append" =>
        OutputMode.Append()
      case _ =>
        OutputMode.Update()
    }
  }

  def process(df: DataFrame)(implicit spark: SparkSession): Dataset[Metrics.MetricAggregation] = {
    import spark.implicits._
    import EventAggregation.transforms._
    import EventAggregation.implicits._

    implicit val metricAggsEncoder: ExpressionEncoder[MetricAggregation] = EventAggregation.implicits.aggreationEncoder
    val watermarkInterval = config.watermarkIntervalMinutes

    // 1. Convert from the kafka data frame to an instance of our Metric
    val groupedMetrics = df.mapPartitions(
      _.flatMap { kd =>
        bytesToMetricRow(kd.getAs[Array[Byte]]("value"), windowInterval)
      }
    )(metricEncoder)

    // 1b. apply watermark to ditch updates to said record state and allow for eventual timeout
    // 2. groupBy binary key
    // 3. aggregate the value of the metric
    // 4. output the final aggregations
    logger.info(s"aggregation outputMode=$outputMode windowIntervalMinutes=${config.windowIntervalMinutes} watermarkIntervalMinutes=$watermarkInterval")
    groupedMetrics
      .withWatermark("timestamp", s"${config.watermarkIntervalMinutes} minutes")
      .groupByKey(_.metricAggregation)
      .flatMapGroupsWithState[Array[Byte], MetricAggregation](outputMode, GroupStateTimeout.EventTimeTimeout())(metricAggregator)
  }

  def metricAggregator(metricAggregation: Array[Byte], metrics: Iterator[ProductMetric], state: GroupState[Array[Byte]]): Iterator[MetricAggregation] = {
    val aggregation = MetricAggregation.parseFrom(metricAggregation)
    val groupingKeyHash = DiscoveryUtils.generateId(metricAggregation)
    logger.info(s"groupingKeyHash=$groupingKeyHash")
    if (state.hasTimedOut) {
      logger.info(s"state.timeout")
      val result = state.getOption match {
        case Some(binaryMetrics) =>
          val finalSketch = DoublesSketch.wrap(Memory.wrap(binaryMetrics))

          if (finalSketch.getN > 0) {
            Iterator(finalAggregation(aggregation, finalSketch))
          } else {
            // if we have no data cause count is zero - then we should emit nothing
            // unless we want dense values
            // this use case would occur if we do a replay too quickly
            Iterator.empty
          }
        case None =>
          Iterator.empty
      }
      state.remove()
      result
    } else {
      val metricData = state.getOption match {
        case Some(lastState) =>
          logger.info(s"recovered the last state")
          // union the last and new sketches
          val lastSketch = DoubleSketch(lastState)
          val currentSketch = DoubleSketch.sketch(k, metrics.map { m => m.value }.toArray)
          val unionSketch = DoubleSketch.union(k, lastSketch, currentSketch)
          // store updated metrics
          logger.info(s"state=merge num.records=${unionSketch.getN}")
          unionSketch.toByteArray(true)
        case None =>
          val s = DoublesSketch.builder().setK(k).build()
          metrics.foreach { m =>
            s.update(m.value)
          }
          logger.info(s"state=new num.records=${s.getN}")
          s.toByteArray(true)
      }
      state.update(metricData)
      val updateTimeout = aggregation.getWindowEnd
      logger.info(s"updating.timeout.timestamp.to=$updateTimeout")
      state.setTimeoutTimestamp(updateTimeout)
      if (outputMode != OutputMode.Append()) {
        Iterator(finalAggregation(aggregation, DoublesSketch.wrap(Memory.wrap(metricData))))
      } else {
          Iterator.empty
      }
    }
  }

  def finalAggregation(aggregation: MetricAggregation, doublesSketch: DoublesSketch): MetricAggregation = {
    val finalAggregation = Metrics.MetricAggregation.newBuilder(aggregation)
      .setSamples(doublesSketch.getN.toInt)
      .setStats(DiscoveryUtils.metricStats(doublesSketch))
      .setHistogram(DiscoveryUtils.histogramStats(doublesSketch))
      .build()
    if (logger.isDebugEnabled) logger.debug(finalAggregation.toString)
    finalAggregation
  }


}
