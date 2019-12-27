package org.digitalpanda.flink.sensor.digestion

import java.time.Instant

import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.util.Collector
import org.digitalpanda.avro.Measure
import org.digitalpanda.flink.common.JobConf
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

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

    LOG.info(s"Job config: ${config}")

    // Set-up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      .enableCheckpointing(config.getLong("checkpoint.period-milli"), CheckpointingMode.EXACTLY_ONCE)
      .setStateBackend(new FsStateBackend(hdfsCheckpointPath(), true))

    // Topology setup
    //  -> Sources
    val rawMeasureStream = env
      .addSource(kafkaConsumer(config.getString("flink.stream.topic.input.raw-measure"), classOf[Measure]))

    // -> Process
    rawMeasureStream
      // https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html#window-functions
      .keyBy("location", "measureType")
      // TODO: Define event time extractor on Measure
      .window(TumblingEventTimeWindows.of(Time.seconds(config.getLong("flink.stream.avg-window.size-sec"))))
      .aggregate(AverageAggregate[Measure, Double](_.getValue), EmitAggregateMeasure())
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

  case class AverageAggregate[IN, N: Numeric](valueExtractor: Function[IN, N] ) extends AggregateFunction[IN, (Double, Long), Double] {

    override def createAccumulator() = (0.0, 0L)

    override def add(measure: IN, acc:  (Double, Long)) =
      (acc._1 + implicitly[Numeric[N]].toDouble(valueExtractor(measure)) // Context bound instead of typecast
        , acc._2 + 1L)

    override def getResult(acc: (Double, Long)) = acc._1 / acc._2

    override def merge(a: (Double, Long), b: (Double, Long)) =
      (a._1 + b._1, a._2 + b._2)
  }

  case class EmitAggregateMeasure() extends ProcessWindowFunction[Double, (Tuple, Measure), Tuple, TimeWindow] {
    def process(key: Tuple, context: Context, averages: Iterable[Double], out: Collector[(Tuple, Measure)]): () =
      out.collect((key,
        Measure
          .newBuilder()
          .setLocation(key.getField(1))
          .setMeasureType(key.getField(2))
          .setTimestamp(Instant.ofEpochMilli(context.window.getStart + (context.window.getEnd - context.window.getStart) / 2L))
          .setValue(averages.iterator.next())
          .build()
      ))
  }


}
