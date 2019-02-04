package com.supergloo

import java.util.Properties

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

object KafkaStreamsTransformations {

  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-transformations")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // Split a KStream based on the supplied predicates into one or more KStream instances. (details)
  // TODO generate api doc with above description once this settles down
  def kStreamBranch(inputTopic1: String,
                    keyFilter1: String,
                    keyFilter2: String,
                    storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    val inputStream: KStream[String, String] = builder.stream(inputTopic1)

    val results: Array[KStream[String, String]] = inputStream.branch(
      (key, value) => key.startsWith(keyFilter1),
      (key, value) => key.startsWith(keyFilter2),
      (key, value) => true
    )

    // TODO - this won't work because hardcoded to fitler1
    results.foreach((k: KStream[String, String]) =>
      k.to("${keyFilter1}-topic")
    )

    // TODO - have to store in order to test
//    val outputTopic: KTable[String, String] =
//      builder.table(
//        outputTopicName,
//        Materialized.as(storeName)
//      )

    builder.build()
  }

}
