package org.digitalpanda.flink.sensor.digestion

import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.java.tuple.Tuple
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

  def main(args: Array[String]): Unit = {

    LOG.info(s"Job config: $config")

    // Set-up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      .enableCheckpointing(config.getLong("checkpoint.period-milli"), CheckpointingMode.EXACTLY_ONCE)
      .setStateBackend(new FsStateBackend(hdfsCheckpointPath(), true))

    buildProcessing(env,
              kafkaValueConsumer(config.getString("flink.stream.topic.input.raw-measure"), classOf[Measure]),
              kafkaKeyedProducer(config.getString("flink.stream.topic.output.processed-measure"), classOf[Measure]))
      .execute(jobName)
  }

  def buildProcessing(env: StreamExecutionEnvironment,
                      rawRecordSource: SourceFunction[Measure],
                      avgRecordSink: SinkFunction[(Tuple, Measure)]): StreamExecutionEnvironment = {

    // Topology setup
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  -> Sources
    val rawMeasureStream = env
      .addSource(rawRecordSource)

    // -> Process
    val avgMeasureStream = rawMeasureStream
      // https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html#window-functions
      .assignTimestampsAndWatermarks(RawMeasureTimestampExtractor())
      .keyBy("location", "measureType")
      .window(TumblingEventTimeWindows.of(Time.seconds(config.getLong("flink.stream.avg-window.size-sec"))))
      .aggregate(AverageAggregate[Measure](_.getValue), EmitAggregateMeasure())

    // -> Sink
    avgMeasureStream
      .addSink(avgRecordSink)
    env
  }

  def kafkaValueConsumer[V <: SpecificRecord](topic: String, tClass: Class[V]) : FlinkKafkaConsumer[V]  =
  //https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html
  //Note: The KafkaDeserializationSchemaWrapper used in FlinkKafkaConsumer only reads the value bytes
    new FlinkKafkaConsumer[V](
      topic,
      ConfluentRegistryAvroDeserializationSchema.forSpecific(
        tClass, jobConf.config.getString("kafka.schema.registry.url")),
      jobConf.kafkaConsumerConfig()
    )

  def kafkaKeyedProducer[V <: SpecificRecord](topic: String, tClass: Class[V]): FlinkKafkaProducer[Pair[Tuple, V]] =
    new FlinkKafkaProducer(
      topic,
      new AvroKeyedSerializationSchema(tClass),
      kafkaProducerConfig()
    )

}
