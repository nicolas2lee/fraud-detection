package tao.fraud.detection

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.clients.consumer.MockConsumer
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tao.fraud.detection.util.CollectSink

class StreamingJobTest extends AnyFlatSpec with Matchers with BeforeAndAfter  {
  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }


  "Flink basic flow" should "launch flink flow" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)

    // values are collected in a static variable
    CollectSink.values.clear()
    // create a stream of custom elements and apply transformations
    env.fromElements(1, 2)
      .map(_.toLong)
      .addSink(new CollectSink())

    // execute
    env.execute()

    // verify your results
    CollectSink.values should contain allOf (2, 3)
  }

}
