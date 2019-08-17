package org.digitalpanda.bigdata.sensor

import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.digitalpanda.backend.data.SensorMeasureType
import org.digitalpanda.backend.data.SensorMeasureType.TEMPERATURE
import org.digitalpanda.backend.data.history.HistoricalDataStorageHelper.{getHistoricalMeasureBlockId, getRangeSelectionCqlQueries}
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing.SECOND_PRECISION_RAW
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Cassandra-datastax implicit functions
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

object SensorDigestion extends App {


  aggregateHistory("01/07/2019 00:00:00","01/07/2019 00:10:00")


  @transient lazy val conf: SparkConf = loadSparkConf()

  @transient lazy val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  def aggregateHistory(start: String, end: String): Unit = {

    def parseDate(date: String): DateTime =
      DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
        .parseDateTime(date).toDateTime(DateTimeZone.forTimeZone(TimeZone.getDefault))
    val startDate = parseDate(start)
    val endDate = parseDate(end)
    print(s"Aggregate the history of interval [$startDate to $endDate[")

    val locatedMeasures = loadLocatedMeasures()
    println(s"Located measures to aggregate: $locatedMeasures")

    //val lines = session.read.textFile("src/main/resources/sensor_measure_history_seconds.csv")
    //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md

    // Query by time range of (location, measure_type) with "push down" views
    //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md#example-using-format-helper-functions

    print(s"Example block query: $getRawSensorMeasureCqlExampleQuery")

    /*
    location
    time_block_id
    measure_type
    bucket
    timestamp

    getRangeSelectionCqlQueries()
    measureHistorySeconds.sqlContext.sql(
      rangeSelection
    )*/
    for (
      (location, measureType) <- locatedMeasures)
    {
      val histSecondsDf = {
        val startBlockId =  getHistoricalMeasureBlockId(startDate.getMillis, SECOND_PRECISION_RAW)
        val endBlockId = getHistoricalMeasureBlockId(endDate.getMillis, SECOND_PRECISION_RAW)
        val hisSecondDFList = {
          for (
            blockId <- Seq(startBlockId, endBlockId);
            bucket <- 0
          ) yield {
            /* Example bloc query:
              SELECT *
              FROM iot.sensor_measure_history_seconds
              WHERE location = 'server-room'
                AND time_block_id = 2986
                AND measure_type = 'TEMPERATURE'
                AND bucket = 0
                AND timestamp >= 1565933646144 AND timestamp <= 1566020046144
             */

            val valueBlockRdd = spark
              .read
              .cassandraFormat("sensor_measure_history_seconds", "iot")
              .load()
              .filter(s"location = '$location'" +
                s" AND time_block_id = $blockId" +
                s" AND measure_type = $measureType" +
                s" AND bucket = $bucket" +
                s" AND timestamp >= $startBlockId AND timestamp < $endBlockId")
              .select("value")
          }
        }

        //TODO: flatmap DFList into single DF....
      }
    }


  }

  case class MeasureHistorySeconds (
    location : Location,
    time_block_id : Long,
    measure_type: SensorMeasureType,
    bucket: Integer,
    timestamp: DateTime,
    value: Double
  )

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

  def getRawSensorMeasureCqlExampleQuery: String = {
    val now = System.currentTimeMillis()
    val yesterday = now - 24 * 3600 * 1000
    getRangeSelectionCqlQueries(
      "server-room",
      TEMPERATURE,
      SECOND_PRECISION_RAW,
      yesterday,
      now).get(0)
  }
}
