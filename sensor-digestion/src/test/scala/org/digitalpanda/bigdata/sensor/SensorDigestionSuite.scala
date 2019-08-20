package org.digitalpanda.bigdata.sensor


import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.digitalpanda.backend.data.SensorMeasureType._

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class SensorDigestionSuite extends FunSuite with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate {

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CassandraConnector(defaultConf)
  initEmbeddedDb()

  var uut : SensorDigestion = new SensorDigestion(defaultConf)

  override def afterAll(): Unit = {
    uut.spark.stop()
  }

  def initEmbeddedDb(): Unit = {
    val initCql = Source.fromURL(getClass.getClassLoader.getResource("init.cql"))
      .getLines.mkString.split(";")
      .map(s => s + ";")
      .toList
    connector.withSessionDo( session => initCql.foreach(session.execute))
  }

  test("Should be able to access Embedded Cassandra Node") {
    assert(connector
      .withSessionDo(session => session.execute("SELECT * FROM system_schema.tables"))
      .all().toString.contains("system_schema"))
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
    val actual = uut.loadLocatedMeasures()

    // Then
    assert(actual === expected)
  }
}