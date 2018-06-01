package com.twilio.open.streaming.trend.discovery.streams

import com.twilio.open.protocol.{Calls, Metrics}
import com.twilio.open.streaming.trend.discovery.{DiscoveryUtils, StreamProcessor}
import com.twilio.open.streaming.trend.discovery.config.AppConfiguration
import com.twilio.open.streaming.trend.discovery.protocol.SparkSqlProtobufEncoders
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object MetricAggregation {
  val logger: Logger = LoggerFactory.getLogger(classOf[MetricAggregation])
  object implicits {
    implicit val callEventEncoder = SparkSqlProtobufEncoders.protobufWithTimestamp[Calls.CallEvent]
    implicit val metricAggregationEncoder: ExpressionEncoder[Metrics.MetricAggregation] = SparkSqlProtobufEncoders.protobufWithTimestamp[Metrics.MetricAggregation]
  }
  //implicit val metricEncoder: Encoder[Metric] = Encoders.product[Metric]
  //implicit val callEventEncoder: Encoder[CallEvent] = Encoders.product[CallEvent]



}

@SerialVersionUID(1L)
case class MetricAggregation(config: AppConfiguration)(implicit spark: SparkSession)
  extends StreamProcessor[Metrics.MetricAggregation] with Serializable {
  import MetricAggregation.logger

  import MetricAggregation.implicits._

  override def process(df: DataFrame)(implicit spark: SparkSession): DataStreamWriter[Metrics.MetricAggregation] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 1. Convert from the kafka data frame to an instance of our CallEvent
    val callEvents: Dataset[Calls.CallEvent] = df.mapPartitions { kd =>
      kd.flatMap { row =>
        Try(Calls.CallEvent.parseFrom(row.getAs[Array[Byte]]("value"))) match {
          case Success(event) => Some(event)
          case Failure(ex) =>
            logger.error(s"Failed to parse binary Calls.CallEvent ${ex.getCause.getMessage}")
            None
        }
      }
    }

    // 2. Enhance the row data with dimensional hash and group by s"metric.dimensional_hash"
    val rawMetrics: Dataset[Metrics.MetricAggregation] = callEvents.mapPartitions { callEventIterator =>
      callEventIterator.map { callEvent =>
        Metrics.MetricAggregation.getDefaultInstance
      }
    }
    // 3. Aggregate and generate the quantiles - windowed

    // 4. append mode writing the aggregates
    rawMetrics.writeStream

  }
}
