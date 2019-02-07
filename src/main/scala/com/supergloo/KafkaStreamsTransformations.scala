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

  /**
    *
    * Split a KStream based on the supplied predicates into three
    * KStream instances. We only care about the two input filters though
    *
    * @param inputTopic1
    * @param keyFilter1
    * @param keyFilter2
    * @param storeName
    * @return
    */
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

    results(0).to(s"${keyFilter1}-topic")
    results(1).to(s"${keyFilter2}-topic")

    val outputTopicOne: KTable[String, String] =
      builder.table(
        s"${keyFilter1}-topic",
        Materialized.as(s"${keyFilter1}-${storeName}")
      )

    val outputTopicTwo: KTable[String, String] =
      builder.table(
        s"${keyFilter2}-topic",
        Materialized.as(s"${keyFilter2}-${storeName}")
      )

    builder.build()
  }

}
