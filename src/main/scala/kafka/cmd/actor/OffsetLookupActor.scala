package kafka.cmd.actor

import java.sql.Timestamp
import akka.actor.{ActorLogging, Terminated}
import akka.event.LoggingReceive
import kafka.cmd.common.utils.ConsumerOps.apply
import kafka.cmd.common.utils.Converters._
import kafka.cmd.common.request._
import kafka.cmd.common.TopicPartition
import kafka.cmd.common.request.{FindPartitionOffsetDiffRequest, FindPartitionOffsetDiffResponse, FindPartitionOffsetRequest, FindPartitionOffsetResponse, FindTopicOffsetDiffRequest, FindTopicOffsetDiffResponse, FindTopicOffsetRequest, FindTopicOffsetResponse, OffsetBehindTs}
import kafka.cmd.common.utils.ConsumerCreator
import org.apache.kafka.clients.consumer.KafkaConsumer

/**
  * Created by xhuang on 21/04/2017.
  */
class OffsetLookupActor extends KafkaClientActor with ActorLogging {

  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  override def preStart(): Unit = {
    super.preStart()
    consumer = ConsumerCreator.newStandAloneConsumer()
  }


  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
    log.info("OffsetLookupActor has stopped.")
  }

  override def receive: Receive = LoggingReceive {
    case request: FindPartitionOffsetRequest => maybeResponseException {
      val result = consumer.offsetBehindTime(request.topicPartition, request.ts)
      FindPartitionOffsetResponse(request, result)
    }

    case request: FindTopicOffsetRequest => maybeResponseException {
      val result = consumer.offsetBehindTime(request.topic, request.ts)
      FindTopicOffsetResponse(request, result)
    }

    case request: FindPartitionOffsetDiffRequest => maybeResponseException {
      val startOffset: OffsetBehindTs = consumer.offsetBehindTime(request.tp, request.from)
      val endOffset: OffsetBehindTs = consumer.offsetBehindTime(request.tp, request.to)
      FindPartitionOffsetDiffResponse(request, startOffset, endOffset)
    }

    case request: FindTopicOffsetDiffRequest => maybeResponseException {
      val startOffsets = consumer.offsetBehindTime(request.topic, request.from)
      val endOffsets = consumer.offsetBehindTime(request.topic, request.to)
      FindTopicOffsetDiffResponse(request, startOffsets, endOffsets)
    }

    case Terminated => {
      log.info("OffsetLookupActor is terminating.")
      consumer.close()
    }
  }
}

