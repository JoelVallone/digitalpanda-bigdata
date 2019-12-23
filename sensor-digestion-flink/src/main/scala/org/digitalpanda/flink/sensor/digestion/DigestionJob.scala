package org.digitalpanda.flink.sensor.digestion

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties

import akka.event.slf4j.Logger
import com.typesafe.config.{ConfigFactory, ConfigValue}
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.digitalpanda.avro.Measure
import org.digitalpanda.flink.common.JobConfig
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */
object DigestionJob {

  private val jobConf = JobConfig()

  val LOG: Logger = LoggerFactory.getLogger(DigestionJob.getClass)

  def  inputConsumer[T <: SpecificRecord](topic: String, recordType: Class[T]) : FlinkKafkaConsumer[T] =
    //https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html
    new FlinkKafkaConsumer[T](
      topic,
      ConfluentRegistryAvroDeserializationSchema
        .forSpecific(
          recordType,
          jobConf.config.getString("kafka.schema.registry.url")),
      jobConf.properties("kafka.bootstrap.servers", "flink.stream.group.id"))

  def main(args: Array[String]): Unit = {

    LOG.info(s"Job config: ${jobConf.config}")

    // set up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      .enableCheckpointing(jobConf.config.getLong("checkpoint.period-milli"), CheckpointingMode.EXACTLY_ONCE)
      .setStateBackend(new FsStateBackend(jobConf.hdfsCheckpointPath(), true))

    val rawMeasureStream = env
      // TODO: fix missing avro dependencies
      .addSource(inputConsumer(jobConf.config.getString("flink.stream.topic.input.raw-measure"), Class[Measure]))

    rawMeasureStream.print

    // get input data
    val text = ExecutionEnvironment.getExecutionEnvironment
      .fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // execute and print result
    counts.print()

    // execute program
    //env.execute("Flink Scala API Skeleton")
  }
}
