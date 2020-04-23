package tao.fraud.detection.util


import java.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction


class CollectSink extends SinkFunction[Long] {

  override def invoke(value: Long): Unit = {
    synchronized {
      CollectSink.values.add(value)
    }
  }
}

object CollectSink {
  val values: util.List[Long] = new util.ArrayList[Long]()
}
