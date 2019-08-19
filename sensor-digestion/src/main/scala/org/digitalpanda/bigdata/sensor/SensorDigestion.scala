package org.digitalpanda.bigdata.sensor

import java.util.TimeZone

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.digitalpanda.backend.data.SensorMeasureType
import org.digitalpanda.backend.data.SensorMeasureType.TEMPERATURE
import org.digitalpanda.backend.data.history.HistoricalDataStorageHelper.{getHistoricalMeasureBlockId, getRangeSelectionCqlQueries}
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing.SECOND_PRECISION_RAW
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Cassandra-datastax implicit functions
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

object SensorDigestion {


  /** Main function */
  def main(args: Array[String]): Unit = {
    aggregateHistory("01/07/2019 00:00:00","01/07/2019 00:10:00")
  }


  @transient lazy val conf: SparkConf = loadSparkConf()

  @transient lazy val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  def aggregateHistory(start: String, end: String): Unit = {

    def parseDate(date: String): DateTime =
      DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").parseDateTime(date)
    val startDate = parseDate(start)
    val endDate = parseDate(end)
    println(s"Aggregate the history of interval [$startDate to $endDate[")

    val locatedMeasures = loadLocatedMeasures()
    println(s"Located measures to aggregate: $locatedMeasures")

    //val lines = session.read.textFile("src/main/resources/sensor_measure_history_seconds.csv")
    //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md

    // Query by time range of (location, measure_type) with "push down" views
    //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md#example-using-format-helper-functions

    println(s"Example block query: ${getRawSensorMeasureCqlExampleQuery(startDate, endDate)}")
    /*
    Example query:
    SELECT *
    FROM iot.sensor_measure_history_seconds
    WHERE location = 'server-room'
     AND time_block_id = 2979
     AND measure_type = 'TEMPERATURE'
     AND bucket = 0
     AND timestamp >= '2019-07-01T00:00:00.000+02:00' AND timestamp < '2019-07-01T00:10:00.000+02:00' limit 1;
    */

    for ((location, measureType) <- locatedMeasures){//Seq(("server-room", TEMPERATURE))) {
        import spark.implicits._
        val startBlockId = getHistoricalMeasureBlockId(startDate.getMillis, SECOND_PRECISION_RAW)
        val endBlockId = getHistoricalMeasureBlockId(endDate.getMillis, SECOND_PRECISION_RAW)
        val bucketCount = (endDate.getMillis - startDate.getMillis) / 300000
        Seq(startBlockId, endBlockId)
          .map( blockId =>
            spark
              .read
              .cassandraFormat("sensor_measure_history_seconds", "iot")
              .load()
              .filter(s"location = '$location'" +
                s" AND time_block_id = $blockId" +
                s" AND measure_type = '${measureType.name}'" +
                s" AND bucket = 0" +
                s" AND timestamp >= '$startDate' AND timestamp < '$endDate'") // Range predicate does not seem to be used
              .select(($"timestamp".cast(DataTypes.IntegerType) / bucketCount).cast(DataTypes.IntegerType).as("bucketId"), $"value")
          )
          .reduce((a, b) => a.union(b))
          .groupBy($"bucketId")
          .avg("value")
          .show(2) // returns empty table
    }

    case class Measure (bucketId: Int, value: Double)
  }


  def loadLocatedMeasures() :  Set[(Location, SensorMeasureType)] = {
    import spark.implicits._
    spark
      .read
      .cassandraFormat("sensor_measure_latest", "iot")
      .load()
      .select($"location", $"measure_type")
    .collect()
      .map(row => (row.getString(0), SensorMeasureType.valueOf(row.getString(1))))
      .toSet
  }

  def loadSparkConf(): SparkConf = {
    new SparkConf()
      //Spark instance config
      .setMaster("local")
      .setAppName("SensorDigestion")
      .set("spark.driver.bindAddress", "127.0.0.1")
      //Cassandra connection
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "9040")
      .set("spark.cassandra.connection.ssl.enabled", "false")
      //.set("spark.cassandra.auth.username","???")
      //.set("spark.cassandra.auth.password","???")
      //Cassandra throughput-related
      .set("spark.cassandra.output.batch.size.rows", "1")
      .set("spark.cassandra.connection.connections_per_executor_max", "10")
      .set("spark.cassandra.output.concurrent.writes", "1024")
      .set("spark.cassandra.concurrent.reads", "512")
      .set("spark.cassandra.output.batch.grouping.buffer.size", "1024")
      .set("spark.cassandra.connection.keep_alive_ms", "600000")
  }

  def getRawSensorMeasureCqlExampleQuery(startDate: DateTime, endDate: DateTime): String =
    getRangeSelectionCqlQueries(
      "server-room",
      TEMPERATURE,
      SECOND_PRECISION_RAW,
      startDate.getMillis,
      endDate.getMillis).toString
}
