package org.digitalpanda.flink.sensor.digestion.operators

import java.time.Instant

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.digitalpanda.avro.Measure

case class EmitAggregateMeasure() extends ProcessWindowFunction[Double, (Tuple, Measure), Tuple, TimeWindow] {
  def process(key: Tuple, context: Context, aggregate: Iterable[Double], out: Collector[(Tuple, Measure)]): () =
    out.collect((
        key,
        Measure
          .newBuilder()
          .setLocation(key.getField(1))
          .setMeasureType(key.getField(2))
          .setTimestamp(middleWindowTimestamp(context))
          .setValue(aggregate.iterator.next())
          .build()
    ))

  private def middleWindowTimestamp(context: Context): Instant =
    Instant.ofEpochMilli(context.window.getStart + (context.window.getEnd - context.window.getStart) / 2L)
}
