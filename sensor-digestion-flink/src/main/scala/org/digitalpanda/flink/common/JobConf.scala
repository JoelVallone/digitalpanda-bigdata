package org.digitalpanda.flink.common

import java.util.Properties

import com.typesafe.config.ConfigFactory

object JobConf {
  def apply(confResource: String) : JobConf  = JobConf(Option(confResource))
  def apply() : JobConf = JobConf(Option.empty)
}

case class JobConf(confResourceOpt: Option[String]) {

  val config: com.typesafe.config.Config = confResourceOpt.map(path => ConfigFactory.load(path)).getOrElse(ConfigFactory.load())

  val KafkaGroupIdKey = "flink.stream.group.id"
  val CheckpointFolderKey = "flink.stream.checkpoint.base-folder"

  def generateConfig(key: String): String = {
    case KafkaGroupIdKey => "org.digitalpanda.flink." + jobName
    case CheckpointFolderKey => jobName
    case _ => config.getString(key)
  }

  def jobName: String =  config.getString("flink.stream.job-name")

  def kafkaConsumerConfig() : Properties = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", generateConfig("base.kafka.bootstrap.servers"))
    prop.setProperty("group.id", generateConfig(KafkaGroupIdKey))
    prop
  }

  def hdfsCheckpointPath() : String =
    generateConfig("base.hdfs.namenode.base-url") + generateConfig(CheckpointFolderKey)


}
