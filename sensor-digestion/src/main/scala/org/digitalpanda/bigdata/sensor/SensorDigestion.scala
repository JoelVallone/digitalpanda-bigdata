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

    val locatedMeasures = digestion.loadLocatedMeasures()

    println(
      s"Aggregate the history for: \n" +
      s" - interval [$begin to $end[ \n" +
      s" - located measures $locatedMeasures")

    println("Example block query: " +
      digestion.getRawSensorMeasureCqlExampleQuery(parseDate(begin), parseDate(end)))

    //digestion.aggregateHistory(begin, end, locatedMeasures)
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

    for ((location, measureType) <- locatedMeasures){//Seq(("server-room", TEMPERATURE))) {
        import spark.implicits._
        val startBlockId = getHistoricalMeasureBlockId(beginDate.getMillis, SECOND_PRECISION_RAW)
        val endBlockId = getHistoricalMeasureBlockId(endDate.getMillis, SECOND_PRECISION_RAW)
        val bucketCount = (endDate.getMillis - beginDate.getMillis) / 300000
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
                s" AND timestamp >= '$beginDate' AND timestamp < '$endDate'") //TODO: fix ->  Range predicate does not seem to be used
              .select(($"timestamp".cast(DataTypes.IntegerType) / bucketCount).cast(DataTypes.IntegerType).as("bucketId"), $"value")
          )
          .reduce((a, b) => a.union(b))
          .groupBy($"bucketId")
          .avg("value")
          .show(2) //TODO: fix -> returns empty table
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

  def getRawSensorMeasureCqlExampleQuery(startDate: DateTime, endDate: DateTime): String =
    getRangeSelectionCqlQueries(
      "server-room",
      TEMPERATURE,
      SECOND_PRECISION_RAW,
      startDate.getMillis,
      endDate.getMillis).toString
}
