package com.twilio.open.streaming.trend.discovery

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait StreamProcessor[T] extends Serializable {

  def process(df: DataFrame)(implicit spark: SparkSession): Dataset[T]

}
