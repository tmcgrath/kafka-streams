package com.supergloo.examples

import com.supergloo.KafkaStreamsJoins
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.{FlatSpec, Matchers}

class KafkaStreamsJoinsSpec extends FlatSpec with Matchers with KafkaTestSetup {

//  import Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import collection.JavaConverters._

  "Inner join" should "save expected results to state store" in {
    val inputTopicOne = "input-topic-1"
    val inputTopicTwo = "input-topic-2"
    val stateStore = "saved-state"

    val driver = new TopologyTestDriver(
      KafkaStreamsJoins.kTableToKTableJoin(inputTopicOne, inputTopicTwo, stateStore),
      config
    )

    val recordFactory = new ConsumerRecordFactory(new StringSerializer(), new StringSerializer())

    // Part 1 - Load up input-topic-1
    val userRegions = scala.collection.mutable.Seq[KeyValue[String, String]](
      ("sensor-1", "MN"),
      ("sensor-2", "WI")
    ).asJava

    driver.pipeInput(recordFactory.create(inputTopicOne, userRegions))

    // Part 2 - Load up input-topic-2
    val userClicks = scala.collection.mutable.Seq[KeyValue[String, java.lang.Long]](
      KeyValue.pair("sensor-1", 99L),
      KeyValue.pair("sensor-2", 1L)
    ).asJava

    val recordFactoryTwo: ConsumerRecordFactory[String, java.lang.Long] =
      new ConsumerRecordFactory(new StringSerializer(), new LongSerializer())

    driver.pipeInput(recordFactoryTwo.create(inputTopicTwo, userClicks))

    // Perform tests
    val store: KeyValueStore[String, String] = driver.getKeyValueStore(stateStore)

    store.get("sensor-1") shouldBe "MN/99"
    store.get("sensor-3") shouldBe null

    driver.close()
  }
}
