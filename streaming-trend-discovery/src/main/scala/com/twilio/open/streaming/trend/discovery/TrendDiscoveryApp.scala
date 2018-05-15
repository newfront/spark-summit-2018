package com.twilio.open.streaming.trend.discovery

import com.twilio.open.streaming.trend.discovery.config.AppConfiguration
import com.twilio.open.streaming.trend.discovery.listeners.SparkApplicationListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object TrendDiscoveryApp {

  val logger: Logger = LoggerFactory.getLogger(classOf[TrendDiscoveryApp])

  def main(args: Array[String]): Unit = {
    logger.info(s"application arguments: $args")
    assert(args.length > 0, "No application config supplied to app args")
    val config = AppConfiguration(args(0))

    val sparkConf = new SparkConf()
      .setAppName(config.appName)
      .setJars(SparkContext.jarOfClass(classOf[TrendDiscoveryApp]).toList)
      .setAll(config.sparkConfig)

    logger.info(s"sparkConfig: ${sparkConf.toDebugString}")

    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport() // *need to build spark with hive jars
      .getOrCreate()

    // Add Listener Interface to gain more information about how Spark is running
    sparkSession.sparkContext.addSparkListener(SparkApplicationListener(sparkSession))

    logger.info(s"warehouse path: ${sparkSession.sharedState.warehousePath}")

  }

}

class TrendDiscoveryApp(override val spark: SparkSession) extends RestartableStreamingApp {
  override val logger: Logger = TrendDiscoveryApp.logger

  lazy val kafkaStreamingSource: DataStreamReader = {

  }

  override def run(): Unit = {

  }
}
