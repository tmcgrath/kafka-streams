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
    val userLastLogins: KTable[String, Long] = builder.table(inputTopic2)

    userRegions.join(userLastLogins,
      Materialized.as(storeName))((regionValue, lastLoginValue) => regionValue + "/" + lastLoginValue)

    builder.build()
  }

  def kTableToKTableJoinOrig(inputTopic1: String,
                         inputTopic2: String,
                         storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder
//    val textLines: KStream[String, String] = builder.stream[String, String](inputTopic)

    val userRegions: KTable[String, String] = builder.table(inputTopic1)
    val userLastLogins: KTable[String, Long] = builder.table(inputTopic2)

//    val wordCounts: KTable[String, Long] = textLines
//      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
//      .groupBy((_, word) => word)
//      .count()(Materialized.as("counts-store"))

//    *     userRegions.join(userLastLogins,
//      * (regionValue, lastLoginValue) -> regionValue + "/" + lastLoginValue,
//      *         Materialized.as(storeName))
//    * .toStream()
//    * .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    userRegions.join(userLastLogins,
      Materialized.as(storeName))((regionValue, lastLoginValue) => regionValue + "/" + lastLoginValue)

//    val joey: KStream[String, String] = userRegions.leftJoin((userLastLogins)((regionValue, lastLoginValue) => regionValue + "/" + lastLoginValue)
//      userRegions.join(userLastLogins, Materialized.as(storeName))((regionValue, lastLoginValue) => regionValue + "/" + lastLoginValue)

//    ((regionValue, lastLoginValue) => regionValue + "/" + lastLoginValue)


//    wordCounts.toStream.to(outputTopic)
    builder.build()
  }

  /**
    * final StreamsBuilder builder = new StreamsBuilder();
    * final KTable<String, String> userRegions = builder.table(userRegionTopic);
    * final KTable<String, Long> userLastLogins = builder.table(userLastLoginTopic, Consumed.with(stringSerde, longSerde));
    *
    * final String storeName = "joined-store";
    *     userRegions.join(userLastLogins,
    * (regionValue, lastLoginValue) -> regionValue + "/" + lastLoginValue,
    *         Materialized.as(storeName))
    * .toStream()
    * .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    */
}
