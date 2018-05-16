package com.twilio.open.streaming.trend.discovery

import com.twilio.open.streaming.trend.discovery.config.Configuration
import com.twilio.open.streaming.trend.discovery.listeners.SparkStreamingQueryListener
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

trait StreamingApp[+Configuration] {
  val config: Configuration
  val logger: Logger
  def run(): Unit
}

trait Restartable {
  def restart(): Unit
}

trait RestartableStreamingApp[T <: Configuration] extends StreamingApp[T] with Restartable {
  val spark: SparkSession

  val streamingQueryListener: SparkStreamingQueryListener = {
    new SparkStreamingQueryListener(spark, restart)
  }

  def monitoredRun(): Unit = {
    run()
    monitorStreams()
  }

  /**
    * Call this from your run method after you've started one or more streaming queries
    */
  def monitorStreams(): Unit = {
    val streams = spark.streams
    streams.addListener(streamingQueryListener)
    streams.awaitAnyTermination()
  }

  /**
    * Call restart
    */
  def restart(): Unit = {
    logger.info(s"restarting the application. cleaning up old stream listener and streams")

    val streams = spark.streams
    streams.removeListener(streamingQueryListener)
    streams.active.foreach { stream =>
      logger.info(s"stream_name=${stream.name} state=active status=${stream.status} action=stop_stream")
      stream.stop()
    }
    logger.info(s"attempting to restart the application")
    monitoredRun()
  }
}
