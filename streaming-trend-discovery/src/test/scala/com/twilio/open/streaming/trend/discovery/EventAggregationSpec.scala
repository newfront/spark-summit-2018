package com.twilio.open.streaming.trend.discovery

import java.util

import com.twilio.open.protocol.Calls.CallEvent
import com.twilio.open.protocol.Metrics
import com.twilio.open.streaming.trend.discovery.streams.EventAggregation
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql._
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

class EventAggregationSpec extends KafkaBackedTest[String, CallEvent] {
  override val testUtils = new KafkaTestUtils[String, CallEvent] {
    override val keySerializer: Serializer[String] = new StringSerializer
    override val keyDeserializer: Deserializer[String] = new StringDeserializer
    override val valueSerializer: Serializer[CallEvent] = new CallEventSerializer
    override val valueDeserializer: Deserializer[CallEvent] = new CallEventDeserializer
  }
  override protected val kafkaTopic = "spark.summit.call.events"
  override protected val partitions = 8

  private val pathToTestScenarios = "src/test/resources/scenarios"

  val log: Logger = LoggerFactory.getLogger(classOf[EventAggregation])

  lazy val session: SparkSession = sparkSql

  override def conf: SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("aggregation-test-app")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      .set("spark.sql.shuffle.partitions", "32")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memory", "1g")
      .set("spark.ui.enabled", "false")
      .setJars(SparkContext.jarOfClass(classOf[EventAggregation]).toList)
  }

  test("Should aggregate call events") {
    import session.implicits._
    val appConfig = appConfigForTest()
    val scenario = TestHelper.loadScenario[CallEvent](s"$pathToTestScenarios/pdd_events.json")
    val scenarioIter = scenario.toIterator
    scenario.nonEmpty shouldBe true

    testUtils.createTopic(kafkaTopic, partitions, overwrite = true)
    sendNextMessages(scenarioIter, 30, _.getEventId, _.getLoggedEventTime)

    val trendDiscoveryApp = new TrendDiscoveryApp(appConfigForTest(), session)
    val eventAggregation = EventAggregation(appConfig)

    eventAggregation.process(trendDiscoveryApp.readKafkaStream())(session)
      .writeStream
      .queryName("calleventaggs")
      .format("memory")
      .outputMode(eventAggregation.outputMode)
      .start()
      .processAllAvailable()

    val df = session.sql("select * from calleventaggs")
    df.printSchema()
    df.show

    val res = session
      .sql("select avg(stats.p99) from calleventaggs")
      .collect()
      .map { r =>
        r.getAs[Double](0) }
      .head

    DiscoveryUtils.round(res) shouldEqual 7.13

  }


}

class CallEventSerializer extends Serializer[CallEvent] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def serialize(topic: String, data: CallEvent): Array[Byte] = data.toByteArray
  override def close(): Unit = {}
}

class CallEventDeserializer extends Deserializer[CallEvent] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def deserialize(topic: String, data: Array[Byte]): CallEvent = CallEvent.parseFrom(data)
  override def close(): Unit = {}
}
