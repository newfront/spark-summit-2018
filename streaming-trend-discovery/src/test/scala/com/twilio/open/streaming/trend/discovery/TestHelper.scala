package com.twilio.open.streaming.trend.discovery

import java.io.{ByteArrayInputStream, InputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.protobuf.Message
import com.googlecode.protobuf.format.JsonFormat
import com.holdenkarau.spark.testing.{LocalSparkContext, SparkContextProvider}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.classTag

object TestHelper {
  val log: Logger = LoggerFactory.getLogger("com.twilio.open.streaming.trend.discovery.TestHelper")
  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
  }

  val jsonFormat: JsonFormat = new JsonFormat

  def loadScenario[T<: Message : ClassTag](file: String): Seq[T] = {
    val fileString = Source.fromFile(file).mkString
    val parsed = mapper.readValue(fileString, classOf[Sceanario])
    parsed.input.map { data =>
      val json = mapper.writeValueAsString(data)
      convert[T](json)
    }
  }

  def convert[T<: Message : ClassTag](json: String): T = {
    val clazz = classTag[T].runtimeClass
    val builder = clazz.getMethod("newBuilder").invoke(clazz).asInstanceOf[Message.Builder]
    try {
      val input: InputStream = new ByteArrayInputStream(json.getBytes())
      jsonFormat.merge(input, builder)
      builder.build().asInstanceOf[T]
    } catch {
      case e: Exception =>
        throw e
    }
  }

}

@SerialVersionUID(1L)
case class KafkaDataFrame(key: Array[Byte], topic: Array[Byte], value: Array[Byte]) extends Serializable

case class Sceanario(input: Seq[Any], expected: Option[Any] = None)

trait SparkSqlTest extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  @transient var _sparkSql: SparkSession = _
  @transient private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  def conf: SparkConf

  def sparkSql: SparkSession = _sparkSql

  override def beforeAll() {
    _sparkSql = SparkSession.builder().config(conf).getOrCreate()

    _sc = _sparkSql.sparkContext
    setup(_sc)
    super.beforeAll()
  }

  override def afterAll() {
    try {
      _sparkSql.close()
      _sparkSql = null
      LocalSparkContext.stop(_sc)
      _sc = null
    } finally {
      super.afterAll()
    }
  }

}
