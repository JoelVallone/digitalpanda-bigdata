package org.digitalpanda.bigdata.sensor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SensorDigestion {

  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("SensorDigestion")
    .set("spark.driver.bindAddress", "127.0.0.1")

  @transient lazy val sc: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    println("Started SensorDigestion")
  }
}
