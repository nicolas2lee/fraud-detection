/*
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

package tao.fraud.detection

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {

  def setupKafkaSource(): FlinkKafkaConsumer[String] = {
    //ParameterTool.fromPropertiesFile("classpath:application.properties")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "broker-3-ymx01nzyk2ttm82k.kafka.svc01.eu-de.eventstreams.cloud.ibm.com:9093,broker-0-ymx01nzyk2ttm82k.kafka.svc01.eu-de.eventstreams.cloud.ibm.com:9093,broker-1-ymx01nzyk2ttm82k.kafka.svc01.eu-de.eventstreams.cloud.ibm.com:9093,broker-4-ymx01nzyk2ttm82k.kafka.svc01.eu-de.eventstreams.cloud.ibm.com:9093,broker-2-ymx01nzyk2ttm82k.kafka.svc01.eu-de.eventstreams.cloud.ibm.com:9093,broker-5-ymx01nzyk2ttm82k.kafka.svc01.eu-de.eventstreams.cloud.ibm.com:9093")
    properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"token\" password=\"HANSIZgkFDdEqumbs6_O0AgzBxxgW0Pdxq6lPBDNNVu8\";")
    properties.setProperty("sasl.mechanism", "PLAIN")
    properties.setProperty("security.protocol", "SASL_SSL")
    properties.setProperty("ssl.protocol", "TLSv1.2")
    properties.setProperty("group.id", "myGroup")
    return new FlinkKafkaConsumer[String]("payment", new SimpleStringSchema(), properties)
  }

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * https://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    val kafkaSource: DataStream[String] = env
      .addSource(setupKafkaSource())
        .name("kafka source")
    //kafkaSource.process()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
