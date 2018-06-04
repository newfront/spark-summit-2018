package com.twilio.open.streaming.trend.discovery

import java.io._

import com.google.common.hash.Hashing
import com.google.common.io.BaseEncoding
import com.twilio.open.protocol.Metrics
import com.twilio.open.protocol.Metrics.Window
import com.yahoo.sketches.quantiles.DoublesSketch
import org.apache.spark.streaming.Duration
import org.joda.time.{DateTime, DateTimeZone, Interval}


object DiscoveryUtils {

  def generateId(bytes: Array[Byte]): String = {
    val hf = Hashing.murmur3_128()
    val hc = hf.hashBytes(bytes)
    BaseEncoding.base64Url()
      .omitPadding()
      .encode(hc.asBytes())
  }

  def serialize[T<: Product with Serializable](data: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(data)
    oos.close()
    baos.close()
    baos.toByteArray
  }

  def deserialize[T<: Product with Serializable](data: Array[Byte]): T = {
    var bais = new ByteArrayInputStream(data)
    val ois = new ObjectInputStream(bais)
    val result = ois.readObject().asInstanceOf[T]
    bais.close()
    ois.close()
    result
  }

  /**
    * @param timestamp The timestamp to bucket with
    * @param interval The duration of the window (supporting up to 24 hours for now)
    * @return The Window
    */
  def eventWindow(timestamp: Long, interval: Duration): Window = {

    val dt = new DateTime(timestamp, DateTimeZone.UTC)
    val intervalMinutes = (interval.milliseconds / 1000 / 60).toInt
    val intervalHours = intervalMinutes / 60

    val isHourly = intervalHours > 0
    val isDaily = isHourly && intervalHours >= 24

    val minutesOfHour = dt.getMinuteOfHour
    val startOfHour = dt.minusMinutes(minutesOfHour).minusSeconds(dt.getSecondOfMinute).minusMillis(dt.getMillisOfSecond)

    val windowStart = if (!isHourly) {
      // if subdivided the hour - then we need to pad to closest number of multipliers
      // eg. if 5m window, then
      if (intervalMinutes > 0) {
        val offsetStart = startOfHour.plusMinutes(intervalMinutes * (minutesOfHour / intervalMinutes))
        offsetStart
      } else {
        startOfHour
      }
    } else {
      // get start of the hour (after subtracking the hours)
      val start = if (intervalHours > 1) {
        startOfHour.minusHours(intervalHours.toInt)
      } else {
        startOfHour
      }

      if (isDaily) {
        // pad to the day (if daily)
        start.minusHours(start.getHourOfDay)
      } else {
        start
      }
    }

    val windowEnd = windowStart.plusMinutes(intervalMinutes)
    val windowInterval = new Interval(windowStart.getMillis, windowEnd.getMillis).toDuration

    val formattedInterval = if (!isHourly) {
      windowInterval.getStandardMinutes + "m"
    } else if (isHourly && !isDaily) {
      windowInterval.getStandardHours + "h"
    } else {
      windowInterval.getStandardDays + "d"
    }

    Window.newBuilder()
      .setStartMs(windowStart.getMillis)
      .setEndMs(windowEnd.getMillis)
      .setInterval(formattedInterval)
      .build()
  }

  private[this] final val emptyMetrics: Metrics.Stats = Metrics.Stats.newBuilder()
    .setMin(0)
    .setP25(0)
    .setMean(0)
    .setMedian(0)
    .setP75(0)
    .setP95(0)
    .setP99(0)
    .setMax(0)
    .setVariance(0)
    .setSd(0)
    .build()

  /**
    * Get and round sketch values
    * @param sketch The DoublesSketch
    * @return The MetricStats from the sketch
    */
  def metricStats(sketch: DoublesSketch): Metrics.Stats = {
    val n = sketch.getN
    if (n > 0) {
      // if we have less than K (then use that value) else use K (for closer approximations)
      val samples = if (n < sketch.getK) n else sketch.getK
      val approxValues = sketch.getQuantiles(samples.toInt)
      val approxMean = mean(approxValues)
      val approxVariance = variance(approxValues)
      val approxSd = sd(approxVariance)

      Metrics.Stats.newBuilder()
        .setMin(round(sketch.getMinValue))
        .setP25(round(sketch.getQuantile(0.25)))
        .setMedian(round(sketch.getQuantile(0.50)))
        .setP75(round(sketch.getQuantile(0.75)))
        .setP90(round(sketch.getQuantile(0.90)))
        .setP95(round(sketch.getQuantile(0.95)))
        .setP99(round(sketch.getQuantile(0.99)))
        .setMax(round(sketch.getMaxValue))
        .setMean(round(approxMean))
        .setVariance(round(approxVariance))
        .setSd(round(approxSd))
        .build()
    } else emptyMetrics
  }

  private[this] final val emptyHistogram: Metrics.Histogram = {
    Metrics.Histogram.newBuilder()
      .setBin1(0d)
      .setBin2(0d)
      .setBin3(0d)
      .setBin4(0d)
      .setBin5(0d)
      .setBin6(0d)
      .build()
  }
  private[this] final val percentMassDist: Array[Double] = Array(-2, -1, 0, 1, 2)
  def histogramStats(sketch: DoublesSketch): Metrics.Histogram = {
    val n = sketch.getN
    if (n > 0) {
      val histogram = toHistogram(sketch, percentMassDist)
      Metrics.Histogram.newBuilder()
        .setBin1(histogram(0))
        .setBin2(histogram(1))
        .setBin3(histogram(2))
        .setBin4(histogram(3))
        .setBin5(histogram(4))
        .setBin6(histogram(5))
        .build()
    } else emptyHistogram
  }

  def toHistogram(sketch: DoublesSketch, buckets: Array[Double]): Array[Double] = {
    val probDensity = sketch.getPMF(buckets)
    val approxSize = sketch.getN.toDouble
    val histogram = probDensity.map { density =>
      density * approxSize
    }
    histogram
  }

  def round(value: Double): Double = {
    if (value == 0) value else Math.round(value * 100.0)/100.0
  }

  /**
    * get the variance of the data set
    * @param data The values in the data set
    * @return The variance of the data set or 0
    */
  def variance(data: Array[Double]): Double = {
    if (data.nonEmpty) {
      val length = data.length.toDouble
      val mean = data.sum / length
      val sumOfSquares = data.foldLeft(0.0)((current: Double, next: Double) => {
        current + Math.pow(next - mean, 2)
      })
      sumOfSquares / (length-1)
    } else 0.0
  }

  /**
    * get the standard deviation of the variance (sqrt(variance))
    * @param variance The value of the variance of the data set
    * @return The standard deviation of the data set
    */
  def sd(variance: Double): Double = Math.sqrt(variance)

  /**
    * Get the mean value (average) in the data set
    * @param data The values in the data set
    * @return The mean value or 0
    */
  def mean(data: Array[Double]): Double = {
    if (data.nonEmpty) {
      data.sum / data.length
    } else 0.0
  }

}
