package org.digitalpanda.flink.test

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

import org.digitalpanda.avro.{Measure, MeasureType}

object TestHelper {

  def measure(location: String, measureType: MeasureType, zuluTime: String, value: Double): Measure = {
    val time = ZonedDateTime.parse(zuluTime, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()))
    Measure.newBuilder()
      .setLocation(location)
      .setMeasureType(measureType)
      .setTimestamp(time.toInstant)
      .setValue(value)
      .build()
  }

}