package org.digitalpanda.bigdata.sensor


import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.digitalpanda.backend.data.SensorMeasureType._

@RunWith(classOf[JUnitRunner])
class SensorDigestionSuite extends FunSuite with BeforeAndAfterAll {

  def initializeSensorDigestion(): Boolean =
    try {
      SensorDigestion
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    SensorDigestion.spark.stop()
  }

  /*
    TODO: use embedded cassandra for tests
   */
  test("'loadLocatedMeasures' - loads set from Cassandra embedded DB ") {
    // Given
    val expected = Set(
      ("server-room",PRESSURE),
      ("server-room", TEMPERATURE),
      ("outdoor", PRESSURE),
      ("outdoor", TEMPERATURE),
      ("outdoor", HUMIDITY))


    // When
    val actual = SensorDigestion.loadLocatedMeasures()

    // Then
    assert(actual === expected)
  }
}