package com.twilio.open.streaming.trend.discovery.config

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

object AppConfiguration {

  private val mapper = new ObjectMapper(new YAMLFactory)

  @volatile
  private var config: AppConfiguration = _

  def apply(resourcePath: String): AppConfiguration = {
    if (config == null)
      synchronized {
        if (config == null)
          config = parse(resourcePath)
      }
    config
  }

  private def parse(resourcePath: String): AppConfiguration = {
    mapper.readValue(new File(resourcePath), classOf[AppConfiguration])
  }

}

sealed trait Configuration

@SerialVersionUID(100L)
case class AppConfiguration(
  appName: String,
  triggerInterval: String,
  sparkConfig: Map[String, String],
  @JsonProperty("kafkaReader") kafkaDataStreamReader: KafkaReaderOrWriterConfig)
  extends Configuration with Serializable

trait KafkaConfig {
  val topic: String
  val subscriptionType: String
  val conf: Map[String, String]
}

case class KafkaReaderOrWriterConfig(
  override val topic: String,
  override val subscriptionType: String,
  override val conf: Map[String, String]) extends KafkaConfig with Serializable
