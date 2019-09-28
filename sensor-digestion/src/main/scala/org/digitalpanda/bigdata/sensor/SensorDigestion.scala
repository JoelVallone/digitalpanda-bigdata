package org.digitalpanda.bigdata.sensor

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.DataTypes
import org.digitalpanda.backend.data.SensorMeasureType
import org.digitalpanda.backend.data.SensorMeasureType.TEMPERATURE
import org.digitalpanda.backend.data.history.HistoricalDataStorageHelper.{getHistoricalMeasureBlockId, getRangeSelectionCqlQueries}
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing.{MINUTE_PRECISION_AVG, SECOND_PRECISION_RAW}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// Cassandra-datastax implicit functions
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._


object SensorDigestion {

  /** Main function */
  def main(args: Array[String]): Unit = {

    val beginDate = parseDate("01/07/2019 00:00:00")
    val endDate = parseDate("01/07/2019 00:10:00")

    val conf = SensorDigestion.loadSparkConf()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val digestion =  new SensorDigestion(spark)

    val locatedMeasures =  Set(("server-room", TEMPERATURE)) //digestion.loadLocatedMeasures() //

    for ((location, measure) <- locatedMeasures) {
      println(
        s"Aggregate the history for: \n" +
          s" - interval [$beginDate to $endDate[ \n" +
          s" - location $location \n" +
          s"-  measure $measure")

      digestion.aggregateHistory(
        beginDate, endDate, location, measure,
        SECOND_PRECISION_RAW, MINUTE_PRECISION_AVG)
        .show(20)
    }
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

  def parseDate(date: String): DateTime =
    DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").parseDateTime(date)

  def toCqlTimestamp(date: DateTime) : String =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZZ").print(date)

}

class SensorDigestion(spark: SparkSession){

  import SensorDigestion._

  def aggregateHistory(beginDate: DateTime,
                       endDate: DateTime,
                       location: Location,
                       measureType: SensorMeasureType,
                       sourceDataSizing: HistoricalDataStorageSizing,
                       targetDataSizing: HistoricalDataStorageSizing): Dataset[Measure] = {
    import spark.implicits._
    val beginSec = beginDate.getMillis / 1000
    val startBlockId = getHistoricalMeasureBlockId(beginDate.getMillis, sourceDataSizing)
    val endBlockId = getHistoricalMeasureBlockId(endDate.getMillis, sourceDataSizing)
    val aggregateIntervalSec = targetDataSizing.getAggregateIntervalSeconds
    val df = (startBlockId to endBlockId)
      .map( blockId =>  {
        val block = spark
          .read
          .cassandraFormat("sensor_measure_history_seconds", "iot")
          .load()
          //https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkPredicatePushdown.html
          .filter(
            s"location = '$location'" +
            s" AND time_block_id = $blockId" +
            s" AND measure_type = '${measureType.name}'" +
            s" AND timestamp >= cast('${toCqlTimestamp(beginDate)}' as TIMESTAMP)" +
            s" AND timestamp <  cast('${toCqlTimestamp(endDate)}' as TIMESTAMP)" /**/
            )
          .select(
             (($"timestamp".cast(DataTypes.IntegerType) - beginSec) / aggregateIntervalSec)
                .cast(DataTypes.IntegerType).as("bucketId"),
              $"value")
        block.explain
        block
      })
      .reduce((b1, b2) => b1.union(b2))
      .groupBy($"bucketId")
      .avg("value")
      .select(
        $"avg(value)".as("value"),
        ((($"bucketId" + 0.5) * aggregateIntervalSec).cast(DataTypes.LongType) + beginSec.toLong).as("timestamp")
      ).as[Measure]
    df
  }


  def loadLocatedMeasures() :  Set[(Location, SensorMeasureType)] = {
    // https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md
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
}
