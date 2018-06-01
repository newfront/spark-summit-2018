package com.twilio.open.streaming.trend.discovery.streams

import com.twilio.open.streaming.trend.discovery.{DiscoveryUtils, StreamProcessor}
import com.twilio.open.streaming.trend.discovery.config.AppConfiguration
import com.twilio.open.streaming.trend.discovery.protocol.{CallEvent, Metric}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object MetricAggregation {

  val logger: Logger = LoggerFactory.getLogger(classOf[MetricAggregation])



}

@SerialVersionUID(1L)
case class MetricAggregation(config: AppConfiguration)(implicit spark: SparkSession)
  extends StreamProcessor[Metric] with Serializable {
  import MetricAggregation.logger

  implicit val metricEncoder: Encoder[Metric] = Encoders.product[Metric]
  implicit val callEventEncoder: Encoder[CallEvent] = Encoders.product[CallEvent]

  override def process(df: DataFrame): DataStreamWriter[Metric] = {
    import spark.implicits._

    // 1. Convert from the kafka data frame to an instance of our CallEvent
    /*df.mapPartitions { kd =>
      kd.flatMap { row =>
        val raw = row.getAs[Array[Byte]]("value")
        // deserialize scala Product type (CallEvent)
      }
    }*/
    // 2. Enhance the row data with dimensional hash and group by s"metric.dimensional_hash"
    // 3. Aggregate and generate the quantiles - windowed
    // 4. append mode writing the aggregates
    val res: Dataset[Metric] = df.mapPartitions { kd =>
      kd.map { row =>
        row.getAs[Array[Byte]]("value")
      }
    }.map { bytes =>
      DiscoveryUtils.deserialize[Metric](bytes)
    }

    res.writeStream

  }
}
