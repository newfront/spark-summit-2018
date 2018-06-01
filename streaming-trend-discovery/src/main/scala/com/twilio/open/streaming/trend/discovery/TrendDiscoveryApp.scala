package com.twilio.open.streaming.trend.discovery

import com.twilio.open.streaming.trend.discovery.config.AppConfiguration
import com.twilio.open.streaming.trend.discovery.listeners.SparkApplicationListener
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, Trigger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration

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

    // create instance of the monitored application
    new TrendDiscoveryApp(config, sparkSession)
      .monitoredRun()

  }

}

class TrendDiscoveryApp(override val config: AppConfiguration, override val spark: SparkSession)
  extends RestartableStreamingApp[AppConfiguration] {
  override val logger: Logger = TrendDiscoveryApp.logger

  lazy val kafkaStreamingSource: DataStreamReader = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
    /*
     Example of how to specify (via config) starting and ending offset range
    .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
    .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
     */
    val kafkaConfig = config.kafkaDataStreamReader

    val reader = spark.readStream.format("kafka")
    kafkaConfig.conf.foreach { entry =>
      reader.option(entry._1, entry._2)
    }
    reader.option(kafkaConfig.subscriptionType, kafkaConfig.topic)
    reader
  }

  def readKafkaStream(): DataFrame = {
    val stream = kafkaStreamingSource.load()
    stream.printSchema()
    stream
  }

  override def run(): Unit = {
    val triggerInterval = Duration(config.triggerInterval) // fully controllable outside of app
    val processingTrigger = Trigger.ProcessingTime(triggerInterval) // use to control throughput

    // create fresh instance of the TrendAggregator (configurable aggregator, 15m/30m)
    // - foreachWriter -> redis (trends by metric, across dimensional hash)
    // - kafkaWriter -> write aggregates for downstream use
    // feed streaming data into fresh instance of TrendDetective   (broadcast updatable static DataFrame - from redis data)
    // feed final trends and delta (prior n aggs) through p95 threshold (prior k windows) (high/low) -> take high and send to Kafka for processing
  }

}
