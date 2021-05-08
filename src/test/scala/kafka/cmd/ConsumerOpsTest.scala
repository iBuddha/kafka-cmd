package kafka.cmd

import kafka.cmd.common.TopicPartition
import kafka.cmd.common.utils.ConsumerOps._
import kafka.cmd.common.utils.ConsumerCreator
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

/**
  * Created by xhuang on 25/04/2017.
  */
class ConsumerOpsTest extends AnyFlatSpecLike with Matchers with BeforeAndAfter {


  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  before {
    consumer = ConsumerCreator.newStandAloneConsumer()
  }

  after {
    consumer.close()
  }

  "topic" should "found" in {
    consumer.exists("topic-not-existed") shouldBe true
  }

  "partition" should "no existed" in {
    consumer.exists(TopicPartition("topic-not-existed", 1)) shouldBe false

  }

//  "message" should "got" in {
//    consumer.message(TopicPartition("clicks", 0), 0, 5000) shouldBe true
//    println(consumer.message(TopicPartition("clicks", 0), 0, 5000).left.get)
//  }
}
