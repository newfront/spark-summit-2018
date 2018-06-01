package com.twilio.open.streaming.trend.discovery

import java.io._

import com.google.common.hash.Hashing
import com.google.common.io.BaseEncoding


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

}
