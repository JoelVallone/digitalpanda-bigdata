package org.digitalpanda.bigdata.sensor


import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.digitalpanda.backend.data.SensorMeasureType._
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing._
import org.digitalpanda.bigdata.sensor.SensorDigestion.parseDate

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class SensorDigestionSuite extends FunSuite with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate {

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))

  val spark: SparkSession = useSparkConf(defaultConf)
  val connector = CassandraConnector(defaultConf)
  initEmbeddedDb(connector)

  var uut : SensorDigestion = new SensorDigestion(spark)

  override def afterAll(): Unit = {
    spark.stop()
  }

  def initEmbeddedDb(connector: CassandraConnector): Unit = {
    val initCql = Source.fromURL(getClass.getClassLoader.getResource("init.cql"))
      .getLines.mkString.split(";")
      .map(s => s + ";")
      .toList
    connector.withSessionDo( session => initCql.foreach(session.execute))
  }

  ignore("Should be able to access Embedded Cassandra Node") {
    assert(connector
      .withSessionDo(session => session.execute("SELECT * FROM system_schema.tables"))
      .all().toString.contains("system_schema"))
  }

  ignore("'loadLocatedMeasures'  loads set from Cassandra embedded DB ") {
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

  test("'aggregateHistory' computes aggregate for located measure over interval ") {
    // Given
    val location = "server-room"
    val metric = TEMPERATURE
    val beginDate =  parseDate("01/07/2019 00:00:00")
    val endDate =  parseDate("01/07/2019 00:20:00")
    val expected = Set(
      Measure(1561932570, 26.5),
      Measure(1561933170, 40.0)
    )

    // When
    val actual = uut.aggregateHistory(
      beginDate, endDate, location, metric,
      SECOND_PRECISION_RAW, MINUTE_PRECISION_AVG)

    // Then
    assert(actual.collect().toSet === expected)
  }
}