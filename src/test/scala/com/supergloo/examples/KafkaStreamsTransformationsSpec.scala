package com.supergloo.examples

import com.supergloo.{KafkaStreamsJoins, KafkaStreamsTransformations}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.{FlatSpec, Matchers}

class KafkaStreamsTransformationsSpec extends FlatSpec with Matchers with KafkaTestSetup {

    import org.apache.kafka.streams.scala.ImplicitConversions._
    import collection.JavaConverters._

    // test fixtures
    val inputTopicOne = "input-topic-1"
    val inputTopicTwo = "input-topic-2"
    val outputTopic = "output-topic"

    val stateStore = "saved-state"

    // input-topic-1
    val userRegions = scala.collection.mutable.Seq[KeyValue[String, String]](
      ("sensor-1", "MN"),
      ("sensor-2", "WI"),
      ("sensor-3-in-topic-one", "IL")
    ).asJava

    // input-topic-2
    val sensorMetric = scala.collection.mutable.Seq[KeyValue[String, java.lang.Long]](
      KeyValue.pair("sensor-1", 99L),
      KeyValue.pair("sensor-2", 1L),
      KeyValue.pair("sensor-99-in-topic-two", 1L),
      KeyValue.pair("sensor-100-in-topic-two", 100L)
    ).asJava

    val recordFactory = new ConsumerRecordFactory(new StringSerializer(), new StringSerializer())

    val recordFactoryTwo: ConsumerRecordFactory[String, java.lang.Long] =
      new ConsumerRecordFactory(new StringSerializer(), new LongSerializer())


    // -------  KTable to KTable Joins ------------ //
    "KStream join" should "filter streams accordingly" in {

      val driver = new TopologyTestDriver(
        KafkaStreamsTransformations.kStreamBranch(inputTopicOne,
                                                  "sensor",
                                                  "sadf",
                                                  stateStore),
        config
      )

      driver.pipeInput(recordFactory.create(inputTopicOne, userRegions))

      // Perform tests
      val store: KeyValueStore[String, String] = driver.getKeyValueStore(stateStore)

      store.get("sensor-1") shouldBe "MN/99"
      store.get("sensor-3-in-topic-one") shouldBe null
      store.get("sensor-99-in-topic-two") shouldBe null

      driver.close()
    }
}
