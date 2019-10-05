package org.digitalpanda.bigdata.sensor

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.DataTypes
import org.digitalpanda.backend.data.SensorMeasureType
import org.digitalpanda.backend.data.SensorMeasureType.TEMPERATURE
import org.digitalpanda.backend.data.history.{AggregateType, HistoricalDataStorageHelper, HistoricalDataStorageSizing}
import org.digitalpanda.backend.data.history.HistoricalDataStorageHelper.{cqlTableOf, getHistoricalMeasureBlockId}
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

    val locatedMeasures =  Set(("server-room", TEMPERATURE)) //digestion.loadLocatedMeasures()

    def sourceTargetPrecisionPairs(aggregateType: AggregateType) :  List[(HistoricalDataStorageSizing, HistoricalDataStorageSizing)] = {
      import HistoricalDataStorageSizing._
      val sortedSizings = values()
        .filter(s => s.getAggregateType == aggregateType || s == SECOND_PRECISION_RAW)
        .sorted
      val sources = sortedSizings.take(sortedSizings.length-1)
      val targets = sortedSizings.tail
      sources.zip(targets).toList
    }

    import scala.util.control.Breaks._
    breakable {
      for ( aggregateType <- AggregateType.AVERAGE::Nil;
            (sourcePrecision, targetPrecision) <- sourceTargetPrecisionPairs(aggregateType) ;
            (location, measureUnit) <- locatedMeasures) {
        println(
          s"Aggregate the history for: \n" +
            s" - interval: [$beginDate to $endDate[ \n" +
            s" - location: $location \n" +
            s" - aggregate type: $aggregateType \n" +
            s"-  source precision: $sourcePrecision\n" +
            s"-  target precision: $targetPrecision")

        val aggregate: Dataset[AnonymousAggregate] = digestion
          .avgAggregateHistory(beginDate, endDate, location, measureUnit, sourcePrecision, targetPrecision)
          .persist()
        aggregate
          .show(20)
        digestion.saveAggregate(aggregate, location,  measureUnit, targetPrecision)
      }
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

  val cassandra_kespace = "iot"

  import SensorDigestion._

  def saveAggregate(ds: Dataset[AnonymousAggregate],
                    location: String,
                    measureUnit: SensorMeasureType,
                    targetDataSizing: HistoricalDataStorageSizing): Unit = {
    import spark.implicits._
    ds
      .repartition(1) // Write data from only one node to keep insertion order (small data set)
      .map( anonAgg =>
        Aggregate(
          location,
          getHistoricalMeasureBlockId(anonAgg.timestamp),
          measureUnit.name,
          HistoricalDataStorageHelper.SENSOR_MEASURE_DEFAULT_BUCKET_ID,
          anonAgg.timestamp,
          anonAgg.value
        ))
      .write
      .cassandraFormat(cqlTableOf(targetDataSizing), cassandra_kespace)
      .save() //TODO: Continue here with tests...
  }

  def avgAggregateHistory(beginDate: DateTime,
                          endDate: DateTime,
                          location: Location,
                          measureType: SensorMeasureType,
                          sourceDataSizing: HistoricalDataStorageSizing,
                          targetDataSizing: HistoricalDataStorageSizing): Dataset[AnonymousAggregate] = {
    import spark.implicits._
    val beginSec = beginDate.getMillis / 1000
    val startBlockId = getHistoricalMeasureBlockId(beginDate.getMillis, sourceDataSizing)
    val endBlockId = getHistoricalMeasureBlockId(endDate.getMillis, sourceDataSizing)
    val aggregateIntervalSec = targetDataSizing.getAggregateIntervalSeconds
    val df = (startBlockId to endBlockId)
      .map( blockId =>  {
        val block = spark
          .read
          .cassandraFormat(cqlTableOf(sourceDataSizing), cassandra_kespace)
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
      ).as[AnonymousAggregate]
    df
  }


  def loadLocatedMeasures() :  Set[(Location, SensorMeasureType)] = {
    // https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md
    import spark.implicits._
    spark
      .read
      .cassandraFormat("sensor_measure_latest", cassandra_kespace)
      .load()
      .select($"location", $"measure_type")
    .collect()
      .map(row => (row.getString(0), SensorMeasureType.valueOf(row.getString(1))))
      .toSet
  }
}
