package org.digitalpanda.avro.util

import java.io.{ByteArrayOutputStream, IOException}

import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.coders.{AvroCoder, Coder}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class AvroKeyedSerializationSchema[V<: SpecificRecord]() extends KeyedSerializationSchema[Pair[Tuple, V]] {

  val coder: AvroCoder[V] = AvroCoder.of(classOf[V])
  val out: ByteArrayOutputStream = new ByteArrayOutputStream

  override def serializeKey(element: (Tuple, V)): Array[Byte] =
    (0 until element._1.getArity)
      .map(id => element._1.getField(id).toString)
      .mkString("-").getBytes


  override def serializeValue(element: (Tuple, V)): Array[Byte]  = {
    try {
      out.reset()
      coder.encode(element._2, out, Coder.Context.NESTED)
    } catch {
      case e: IOException => throw new RuntimeException("Avro encoding failed.", e)
    }
    out.toByteArray
  }

  override def getTargetTopic(element: (Tuple, V)): String =
    null // we are never overriding the topic

}
