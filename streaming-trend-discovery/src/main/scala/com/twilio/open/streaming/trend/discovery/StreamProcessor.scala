package com.twilio.open.streaming.trend.discovery

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

trait StreamProcessor[T <: Product] extends Product with Serializable {

  def process(df: DataFrame)(implicit spark: SparkSession): DataStreamWriter[T]

}
