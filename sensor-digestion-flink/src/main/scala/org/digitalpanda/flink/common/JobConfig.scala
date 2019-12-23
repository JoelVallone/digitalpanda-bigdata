package org.digitalpanda.flink.common

import java.util.Properties

import com.typesafe.config
import com.typesafe.config.{Config, ConfigFactory}

object JobConfig {
  def apply(confResource: String) : JobConfig  = JobConfig(Option(confResource))
  def apply() : JobConfig = JobConfig(Option.empty)
}

case class JobConfig(confResourceOpt: Option[String]) {

  private val _config: com.typesafe.config.Config = confResourceOpt.map(path => ConfigFactory.load(path)).getOrElse(ConfigFactory.load())

  def config: com.typesafe.config.Config = _config

  def properties(keys: String*): Properties = {
    val prop = new Properties();
    keys.foreach( k => prop.setProperty(k, _config.getString(k)))
    prop
  }

  def hdfsCheckpointPath() : String =
    config.getString("base.hdfs.namenode.base-url") + config.getString("flink.stream.checkpoint.folder-name")

}
