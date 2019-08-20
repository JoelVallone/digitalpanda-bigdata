package org.digitalpanda.bigdata.sensor

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.digitalpanda.backend.data.SensorMeasureType
import org.digitalpanda.backend.data.SensorMeasureType.TEMPERATURE
import org.digitalpanda.backend.data.history.HistoricalDataStorageHelper.{getHistoricalMeasureBlockId, getRangeSelectionCqlQueries}
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing.SECOND_PRECISION_RAW
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// Cassandra-datastax implicit functions
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._


object SensorDigestion {

  /** Main function */
  def main(args: Array[String]): Unit = {

    val begin = "01/07/2019 00:00:00"
    val end = "01/07/2019 00:10:00"

    val sparkConf = loadSparkConf()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val digestion =  new SensorDigestion(spark)

    val locatedMeasures = Set(("server-room", TEMPERATURE))//digestion.loadLocatedMeasures()

    println(
      s"Aggregate the history for: \n" +
      s" - interval [$begin to $end[ \n" +
      s" - located measures $locatedMeasures")

    digestion.aggregateHistory(begin, end, locatedMeasures)
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
}

class SensorDigestion(spark: SparkSession){
  import SensorDigestion._

  def aggregateHistory(begin: String, end: String, locatedMeasures: Set[(Location, SensorMeasureType)]): Unit = {

    val beginDate = parseDate(begin)
    val endDate = parseDate(end)

    for ((location, measureType) <- locatedMeasures) {
        import spark.implicits._
      val beginSec = beginDate.getMillis / 1000
      val startBlockId = getHistoricalMeasureBlockId(beginDate.getMillis, SECOND_PRECISION_RAW)
      val endBlockId = getHistoricalMeasureBlockId(endDate.getMillis, SECOND_PRECISION_RAW)
      val aggregateIntervalSec = 300
      Seq(startBlockId, endBlockId)
        .map( blockId => {

          val dataBlock = spark
            .read
            .cassandraFormat("sensor_measure_history_seconds", "iot")
            .load()
            .filter(s"location = '$location'" +
              s" AND time_block_id = $blockId" +
              s" AND measure_type = '${measureType.name}'" +
              s" AND bucket = 0")
              //+ s" AND timestamp >= '${beginDate.getMillis / 1000}' AND timestamp < '${endDate.getMillis / 1000}'") //TODO: fix ->  Range predicate badly interpreted
            .select(
              (($"timestamp".cast(DataTypes.IntegerType) - beginSec) / aggregateIntervalSec)
                .cast(DataTypes.IntegerType).as("bucketId"),
              $"value")
          dataBlock.show()
          dataBlock
        })
        .reduce((a, b) => a.union(b))
        .groupBy($"bucketId")
        .avg("value")
        .show(2)
    }

    case class Measure (bucketId: Int, value: Double)
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
