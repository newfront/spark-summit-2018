package com.twilio.open.streaming.trend.discovery.config

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

sealed trait Configuration extends Product with Serializable

@SerialVersionUID(101L)
case class AppConfiguration(
  @JsonProperty appName: String,
  @JsonProperty triggerInterval: String, // "30 seconds", "1 minute"
  @JsonProperty outputMode: String,
  @JsonProperty checkpointPath: String,
  @JsonProperty("windowInterval") windowIntervalMinutes: Long,
  @JsonProperty("watermarkInterval") watermarkIntervalMinutes: Long,
  @JsonProperty("core") sparkCoreConfig: Map[String, String],
  @JsonProperty callEventsTopic: KafkaReaderOrWriterConfig)
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

object AppConfig {

  private val mapper = new ObjectMapper(new YAMLFactory)
  mapper.registerModule(DefaultScalaModule)

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


