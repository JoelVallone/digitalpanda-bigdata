package org.digitalpanda.flink.sensor.digestion

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.util.FiniteTestSource
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.digitalpanda.avro.Measure
import org.digitalpanda.avro.MeasureType.{PRESSURE, TEMPERATURE}
import org.digitalpanda.flink.test.TestHelper.measure
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

// https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/testing.html#testing-flink-jobs
class MeasureDigestionJobTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

    val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(1)
      .setNumberTaskManagers(2)
      .build)

    before {
        flinkCluster.before()
    }

    after {
        flinkCluster.after()
    }

    "MeasureDigestionJob pipeline" should "compute 60 seconds averages by <Location, MeasureType>" in {
        // Given
        val env = StreamExecutionEnvironment.getExecutionEnvironment.enableCheckpointing
          env.setParallelism(2)

        val metricInput = new FiniteTestSource(
            measure("server-room",  TEMPERATURE, "2019-06-30T22:09:59Z", 26.0),

            measure("server-room",  TEMPERATURE, "2019-06-30T22:10:00Z", 35.5),
            measure("server-room",  TEMPERATURE, "2019-06-30T22:10:10Z", 30.0),
            measure("server-room",  PRESSURE,    "2019-06-30T22:10:20Z", 789.0),
            measure("server-room",  TEMPERATURE, "2019-06-30T22:10:20Z", 35.0),
            measure("outdoor",      TEMPERATURE, "2019-06-30T22:10:20Z", 5.0),
            measure("server-room",  TEMPERATURE, "2019-06-30T22:10:59Z", 30.0),

            measure("server-room",  TEMPERATURE,  "2019-06-30T22:12:00Z", 30.0),
            measure("server-room",  TEMPERATURE,  "2019-06-30T22:12:59Z", 54.0)
        )
        val avgOutput = new CollectSink()
        CollectSink.values.clear()

        // When
        MeasureDigestionJob
          .windowAverage(env, Time.seconds(60L), metricInput, avgOutput)
          .execute("MeasureDigestionJobUUT")

        // Then
        println(s"Averages: \n${CollectSink.values.mkString("\n")}")
        CollectSink.values.toSet should contain allOf (
          ("server-room-TEMPERATURE", measure("server-room",  TEMPERATURE,"2019-06-30T22:09:30Z", 26.0)),
          ("server-room-TEMPERATURE", measure("server-room",  TEMPERATURE,"2019-06-30T22:10:30Z",32.625)),
          ("outdoor-TEMPERATURE",     measure("outdoor",      TEMPERATURE,"2019-06-30T22:10:30Z",5.0)),
          ("server-room-PRESSURE",    measure("server-room",  PRESSURE,   "2019-06-30T22:10:30Z",789.0)),
          ("server-room-TEMPERATURE", measure("server-room",  TEMPERATURE,"2019-06-30T22:12:30Z",42.0))
        )
    }
}

class CollectSink extends SinkFunction[(String, Measure)] {

  override def invoke(value: (String, Measure)): Unit = {
    synchronized {
      CollectSink.values.append(value)
    }
  }
}

object CollectSink {
  val values: ArrayBuffer[(String, Measure)] = ArrayBuffer()
}