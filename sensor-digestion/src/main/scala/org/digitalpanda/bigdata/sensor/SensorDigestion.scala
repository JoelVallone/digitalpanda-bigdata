package org.digitalpanda.bigdata.sensor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.digitalpanda.backend.data.SensorMeasureType.TEMPERATURE
import org.digitalpanda.backend.data.history.HistoricalDataStorageHelper.getRangeSelectionCqlQueries
import org.digitalpanda.backend.data.history.HistoricalDataStorageSizing.SECOND_PRECISION_RAW

object SensorDigestion {


  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("SensorDigestion")
    .set("spark.driver.bindAddress", "127.0.0.1")

  @transient lazy val session: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    //val lines = session.read.textFile("src/main/resources/sensor_measure_history_seconds.csv")
    print(s"$getRawSensorMeasureCqlExampleQuery")
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
