package com.twilio.open.streaming.trend.discovery.listeners

import kamon.Kamon
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.slf4j.{Logger, LoggerFactory}

object SparkStreamingQueryListener {
  val log: Logger = LoggerFactory.getLogger(classOf[SparkStreamingQueryListener])

  def apply(spark: SparkSession, restart: () => Unit): SparkStreamingQueryListener = {
    new SparkStreamingQueryListener(spark, restart)
  }

}

class SparkStreamingQueryListener(sparkSession: SparkSession, restart: () => Unit) extends StreamingQueryListener {
  import SparkStreamingQueryListener._
  private val streams = sparkSession.streams
  private val defaultTag = Map("app_name" -> sparkSession.sparkContext.appName)


  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    if (log.isDebugEnabled) log.debug(s"onQueryStarted queryName=${event.name} id=${event.id} runId=${event.runId}")
  }

  //https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/streaming/progress.scala
  override def onQueryProgress(progressEvent: QueryProgressEvent): Unit = {
    val progress = progressEvent.progress
    val inputRowsPerSecond = progress.inputRowsPerSecond
    val processedRowsPerSecond = progress.processedRowsPerSecond

    val sources = progress.sources.map { source =>
      val description = source.description
      val startOffset = source.startOffset
      val endOffset = source.endOffset
      val inputRows = source.numInputRows

      s"topic=$description startOffset=$startOffset endOffset=$endOffset numRows=$inputRows"
    }
    Kamon.metrics.histogram("spark.query.progress.processed.rows.rate").record(processedRowsPerSecond.toLong)
    Kamon.metrics.histogram("spark.query.progress.input.rows.rate", defaultTag).record(inputRowsPerSecond.toLong)
    log.info(s"query.progress query=${progress.name} kafka=${sources.mkString(",")} inputRows/s=$inputRowsPerSecond processedRows/s=$processedRowsPerSecond durationMs=${progress.durationMs} sink=${progress.sink.json}")
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    log.warn(s"queryTerminated: $event")
    val possibleStreamingQuery = streams.get(event.id)
    if (possibleStreamingQuery != null) {
      val progress = possibleStreamingQuery.lastProgress
      val sources = progress.sources
      log.warn(s"last.progress.sources sources=$sources")
    }

    event.exception match {
      case Some(exception) =>
        log.warn(s"queryEndedWithException exception=$exception resetting.all.streams")
        restart()
      case None =>
    }
  }
}
