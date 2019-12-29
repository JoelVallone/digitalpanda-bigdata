package org.digitalpanda.flink.sensor.digestion

import java.time.Instant
import java.util.Optional

import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
import org.digitalpanda.avro.Measure
import org.digitalpanda.flink.common.JobConf
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.{SerializationSchema, SimpleStringSchema}
import org.digitalpanda.avro.util.AvroKeyedSerializationSchema
import org.digitalpanda.flink.sensor.digestion.operators.{AverageAggregate, EmitAggregateMeasure, RawMeasureTimestampExtractor}

//https://stackoverflow.com/questions/37920023/could-not-find-implicit-value-for-evidence-parameter-of-type-org-apache-flink-ap
import org.apache.flink.streaming.api.scala._

/**
 * Measure digestion job : compute measure windowed averages
 */

// TODO: Consider using integration test for Job https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/testing.html
object MeasureDigestionJob {

  private val jobConf = JobConf(); import jobConf._
  private val LOG: Logger = LoggerFactory.getLogger(MeasureDigestionJob.getClass)

  def main(args: Array[String]): Unit = {

    LOG.info(s"Job config: $config")

    // Set-up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      .enableCheckpointing(config.getLong("checkpoint.period-milli"), CheckpointingMode.EXACTLY_ONCE)
      .setStateBackend(new FsStateBackend(hdfsCheckpointPath(), true))

    // Topology setup
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  -> Sources
    val rawMeasureStream = env
      .addSource(kafkaValueConsumer[Measure](config.getString("flink.stream.topic.input.raw-measure")))
      .assignTimestampsAndWatermarks(RawMeasureTimestampExtractor())

    // -> Process
    val avgMeasureStream = rawMeasureStream
      // https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html#window-functions
      .keyBy("location", "measureType")
      .window(TumblingEventTimeWindows.of(Time.seconds(config.getLong("flink.stream.avg-window.size-sec"))))
      .aggregate(AverageAggregate[Measure](_.getValue), EmitAggregateMeasure())

    // -> Sink
    avgMeasureStream
      .addSink(kafkaKeyedProducer[Measure](config.getString("flink.stream.topic.output.processed-measure")))

    // Execute program
    env.execute(jobName)
  }

  def kafkaValueConsumer[V <: SpecificRecord](topic: String) : FlinkKafkaConsumer[V]  =
  //https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html
  //Note: The KafkaDeserializationSchemaWrapper used in FlinkKafkaConsumer ignores the key and its schema
    new FlinkKafkaConsumer[V](
      topic,
      ConfluentRegistryAvroDeserializationSchema.forSpecific(
        classOf[V], jobConf.config.getString("kafka.schema.registry.url")),
      jobConf.kafkaConsumerConfig()
    )

  def kafkaKeyedProducer[V <: SpecificRecord](topic: String): FlinkKafkaProducer[Pair[Tuple, V]] =
    new FlinkKafkaProducer(
      topic,
      new AvroKeyedSerializationSchema[V](),
      kafkaProducerConfig()
    )

}
