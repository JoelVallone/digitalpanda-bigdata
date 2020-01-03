package org.digitalpanda.flink.sensor.digestion

import org.apache.avro.specific.SpecificRecord
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.digitalpanda.avro.Measure
import org.digitalpanda.avro.util.AvroKeyedSerializationSchema
import org.digitalpanda.flink.common.JobConf
import org.digitalpanda.flink.sensor.digestion.operators.{AverageAggregate, EmitAggregateMeasure, RawMeasureTimestampExtractor}
import org.slf4j.{Logger, LoggerFactory}

//https://stackoverflow.com/questions/37920023/could-not-find-implicit-value-for-evidence-parameter-of-type-org-apache-flink-ap
import org.apache.flink.streaming.api.scala._

/**
 * Measure digestion job : compute measure windowed averages
 */

object MeasureDigestionJob {

  private val jobConf = JobConf(); import jobConf._
  private val LOG: Logger = LoggerFactory.getLogger(MeasureDigestionJob.getClass)

  //TODO: Test on real environment
  def main(args: Array[String]): Unit = {

    LOG.info(s"Job config: $config")

    // Set-up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      .enableCheckpointing(config.getLong("checkpoint.period-milli"), CheckpointingMode.EXACTLY_ONCE)
      .setStateBackend(new FsStateBackend(hdfsCheckpointPath(), true))

    jobConf.forEach("flink.stream.average-digests"){
      windowConf =>
        windowAverage(env,
          Time.seconds(windowConf.getLong("window-size-sec")),
          kafkaValueConsumer(windowConf.getString("topic.input"), classOf[Measure]),
          kafkaKeyedProducer(windowConf.getString("topic.output"), classOf[Measure]))}

    env.execute(jobName)
  }

  def windowAverage(env: StreamExecutionEnvironment,
                    windowSize: Time,
                    metricInput: SourceFunction[Measure],
                    avgOutput: SinkFunction[(String, Measure)]): StreamExecutionEnvironment = {

    // Topology setup
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  -> Sources
    val rawMeasureStream = env
      .addSource(metricInput)

    // -> Process
    val avgMeasureStream = rawMeasureStream
      // https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html#window-functions
      .assignTimestampsAndWatermarks(RawMeasureTimestampExtractor())
      .keyBy("location", "measureType")
      .window(TumblingEventTimeWindows.of(windowSize))
      .aggregate(AverageAggregate[Measure](_.getValue), EmitAggregateMeasure())

    // -> Sink
    avgMeasureStream
      .addSink(avgOutput)
    env
  }

  def kafkaValueConsumer[V <: SpecificRecord](topic: String, tClass: Class[V]) : FlinkKafkaConsumer[V]  =
  //https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html
  //Note: The KafkaDeserializationSchemaWrapper used in FlinkKafkaConsumer only reads the value bytes
    new FlinkKafkaConsumer(
      topic,
      ConfluentRegistryAvroDeserializationSchema.forSpecific(
        tClass, jobConf.config.getString("kafka.schema.registry.url")),
      jobConf.kafkaConsumerConfig()
    )

  def kafkaKeyedProducer[V <: SpecificRecord](topic: String, tClass: Class[V]): FlinkKafkaProducer[Pair[String, V]] =
    new FlinkKafkaProducer(
      topic,
      new AvroKeyedSerializationSchema(tClass),
      kafkaProducerConfig()
    )

}
