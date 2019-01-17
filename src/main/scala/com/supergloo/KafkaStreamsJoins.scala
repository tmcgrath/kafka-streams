package com.supergloo

import java.util.Properties

import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

object KafkaStreamsJoins {

  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-examples")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  def kTableToKTableJoin(inputTopic1: String,
                         inputTopic2: String,
                         storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    val userRegions: KTable[String, String] = builder.table(inputTopic1)
    val regionMetrics: KTable[String, Long] = builder.table(inputTopic2)

    userRegions.join(regionMetrics,
      Materialized.as(storeName))((regionValue, metricValue) => regionValue + "/" + metricValue)

    builder.build()
  }

  def kTableToKTableLeftJoin(inputTopic1: String,
                         inputTopic2: String,
                         storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    val userRegions: KTable[String, String] = builder.table(inputTopic1)
    val regionMetrics: KTable[String, Long] = builder.table(inputTopic2)

    userRegions.leftJoin(regionMetrics,
      Materialized.as(storeName))((regionValue, metricValue) => regionValue + "/" + metricValue)

    builder.build()
  }

  def kTableToKTableOuterJoin(inputTopic1: String,
                             inputTopic2: String,
                             storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    val userRegions: KTable[String, String] = builder.table(inputTopic1)
    val regionMetrics: KTable[String, Long] = builder.table(inputTopic2)

    userRegions.outerJoin(regionMetrics,
      Materialized.as(storeName))((regionValue, metricValue) => regionValue + "/" + metricValue)

    builder.build()
  }

  def kStreamToKTableJoin(inputTopic1: String,
                          inputTopic2: String,
                          outputTopicName: String,
                          storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    val userRegions: KTable[String, String] = builder.table(inputTopic1)
    val regionMetrics: KStream[String, Long] = builder.stream(inputTopic2)

    regionMetrics.join(userRegions){(regionValue, metricValue) =>
      regionValue + "/" + metricValue
    }.to(outputTopicName)

    val outputTopic: KTable[String, String] =
      builder.table(
        outputTopicName,
        Materialized.as(storeName)
      )

    builder.build()
  }

}
