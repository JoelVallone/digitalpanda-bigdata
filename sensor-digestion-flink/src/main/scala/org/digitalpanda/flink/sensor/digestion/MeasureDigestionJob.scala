package org.digitalpanda.flink.sensor.digestion

import org.apache.avro.specific.SpecificRecord
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.digitalpanda.avro.Measure
import org.digitalpanda.flink.common.JobConf
import org.slf4j.{Logger, LoggerFactory}

/**
 * Measure digestion job : compute measure windowed averages
 */

// TODO: Consider using integration test for Job https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/testing.html
object MeasureDigestionJob {

  private val jobConf = JobConf()
  private val LOG: Logger = LoggerFactory.getLogger(MeasureDigestionJob.getClass)

  def main(args: Array[String]): Unit = {
    import jobConf._

    LOG.info(s"Job config: ${config}")

    // Set-up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      .enableCheckpointing(config.getLong("checkpoint.period-milli"), CheckpointingMode.EXACTLY_ONCE)
      .setStateBackend(new FsStateBackend(hdfsCheckpointPath(), true))

    // Topology setup
    //  -> Sources
    val rawMeasureStream = env
      .addSource(kafkaConsumer(config.getString("flink.stream.topic.input.raw-measure"), Class[Measure]))

    rawMeasureStream.print

    // -> Process
    rawMeasureStream
      // https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html
      .keyBy("location", "measureType")
      // TODO: Define event time extractor on Measure
      .window(TumblingEventTimeWindows.of(Time.seconds(config.getLong("flink.stream.avg-window.size-sec"))))
      // TODO: Continue processing here...

    // -> Sink

    // Execute program
    env.execute(jobName)
  }

  def  kafkaConsumer[T <: SpecificRecord](topic: String, recordType: Class[T]) : FlinkKafkaConsumer[T] =
  //https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html
    new FlinkKafkaConsumer[T](
      topic,
      ConfluentRegistryAvroDeserializationSchema
        .forSpecific(recordType, jobConf.config.getString("kafka.schema.registry.url")),
      jobConf.kafkaConsumerConfig())
}
