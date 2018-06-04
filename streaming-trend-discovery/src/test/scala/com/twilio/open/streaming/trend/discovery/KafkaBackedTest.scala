package com.twilio.open.streaming.trend.discovery

import java.nio.file.Files

import com.twilio.open.streaming.trend.discovery.config.{AppConfig, AppConfiguration}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.scalatest.{FunSuite, Matchers}

abstract class KafkaBackedTest[A, B] extends FunSuite with Matchers with SparkSqlTest {

  def testUtils: KafkaTestUtils[A, B]

  protected val checkpointDir: String = Files.createTempDirectory(appID).toString

  protected val kafkaTopic = ""
  protected val partitions = 2

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      super.afterAll()
    }
  }

  def appConfigForTest(): AppConfiguration = {
    val baseConfig = AppConfig("src/test/resources/app.yaml")

    baseConfig.copy(
      checkpointPath = checkpointDir,
      callEventsTopic = baseConfig.callEventsTopic.copy(
        conf = Map(
          "kafka.bootstrap.servers" -> testUtils.brokerAddress,
          "startingOffsets" -> "earliest",
          "maxOffsetsPerTrigger" -> "10"
        )
      )
    )
  }

  def sendNextMessages(events: Iterator[B], upto: Int, keyExtractor: (B) => A,
                       timestamp: (B) => Long): Unit = {
    val nextEvents = 0.to(upto).flatMap { _ =>
      if (events.hasNext) {
        Some(events.next())
      } else {
        None
      }
    }
    testUtils.sendMessages(kafkaTopic, nextEvents, keyExtractor,
      partition = Some(randomInt(partitions-1))
    )
  }

  def randomInt(bounds: Int): Int = {
    (math.random * bounds - 1).toInt
  }

}
