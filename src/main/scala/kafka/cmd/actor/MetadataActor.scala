package kafka.cmd.actor

import akka.actor.{ActorLogging, Terminated}
import akka.event.LoggingReceive
import kafka.cmd.common.request
import kafka.cmd.common.request._
import kafka.cmd.common.TopicPartition
import kafka.cmd.common.exception.RequestExecutionException
import kafka.cmd.common.request.{DescribeTopicRequest, DescribeTopicResponse, ListTopicResponse, ListTopicsRequest, TopicExistRequest, TopicExistResponse, TopicMeta}
import kafka.cmd.common.utils.ConsumerCreator
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by xhuang on 21/04/2017.
  */
class MetadataActor extends KafkaClientActor with ActorLogging {

  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  override def preStart(): Unit = {
    super.preStart()
    consumer = ConsumerCreator.newStandAloneConsumer()
    log.info("MetadataActor started")
  }


  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
    log.info("MetadataActor has stopped.")
  }

  override def receive: Receive = LoggingReceive {
    case r@DescribeTopicRequest(id, topic) => sender ! DescribeTopicResponse(r, Try {
      if (!consumer.listTopics().containsKey(topic)) throw new RequestExecutionException("not such topic")
      val partitionInfo = consumer.partitionsFor(topic).toList
      val partitions = partitionInfo.map(p => new org.apache.kafka.common.TopicPartition(r.topic, p.partition()))
      val beginOffsets = mapAsScalaMap(consumer.beginningOffsets(partitions)).map {
        case (tp, offset) => (TopicPartition(tp.topic(), tp.partition()), offset.toLong)
      }.toMap
      val endOffsets = mapAsScalaMap(consumer.endOffsets(partitions)).map {
        case (tp, offset) => (TopicPartition(tp.topic(), tp.partition()), offset.toLong)
      }.toMap
      TopicMeta(partitionInfo, beginOffsets, endOffsets)
    })

    case r@TopicExistRequest(id, topic) => {
      sender ! TopicExistResponse(r, consumer.listTopics().containsKey(topic))
    }
    case r: ListTopicsRequest => sender ! ListTopicResponse(r, Try{
      val topics = consumer.listTopics()
        .map(_._1)
        .toList
        topics
    }
    )
  }
}


