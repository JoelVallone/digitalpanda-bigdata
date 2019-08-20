package org.digitalpanda.bigdata.sensor


import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.digitalpanda.backend.data.SensorMeasureType._

@RunWith(classOf[JUnitRunner])
class SensorDigestionSuite extends FunSuite with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate{

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  val connector = CassandraConnector(defaultConf)

  /*
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
  }*/

  test("Should be able to access Embedded Cassandra Node") {
    println(connector
      .withSessionDo(session => session.execute("SELECT * FROM system_schema.tables"))
      .all())

  }

  /*
    TODO: use embedded cassandra for tests
   */
  ignore("'loadLocatedMeasures' - loads set from Cassandra embedded DB ") {
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