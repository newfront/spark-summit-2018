package com.twilio.open.streaming.trend.discovery.sketches

import com.yahoo.memory.Memory
import com.yahoo.sketches.quantiles.{DoublesSketch, DoublesUnion}

//https://datasketches.github.io/
//https://github.com/DataSketches/sketches-core/tree/master/src/main/java/com/yahoo/sketches/quantiles
object DoubleSketch {

  val k: Int = 512

  def apply(serialized: Array[Byte]): DoublesSketch = {
    DoublesSketch.wrap(Memory.wrap(serialized))
  }

  def sketch(data: Array[Double]): DoublesSketch = {
    sketch(k, data)
  }

  def sketch(k: Int, data: Array[Double]): DoublesSketch = {
    val s = DoublesSketch.builder().setK(k).build()
    data.foreach { s.update }
    s
  }

  def union(k: Int, a: Array[Double], b: Array[Double]): DoublesSketch = {
    val u = DoublesUnion.builder().setMaxK(k).build()
    u.update(sketch(k, a))
    u.update(sketch(k, b))
    u.getResult
  }

  def union(k: Int, a: DoublesSketch, b: DoublesSketch): DoublesSketch = {
    val u = DoublesUnion.builder().setMaxK(k).build()
    u.update(a)
    u.update(b)
    u.getResult
  }

}
