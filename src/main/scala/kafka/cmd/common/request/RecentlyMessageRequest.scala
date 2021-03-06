package kafka.cmd.common.request

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

/**
  * Created by xhuang on 27/04/2017.
  */
case class RecentlyMessageRequest(id: Long, topic: String) extends KafkaActorRequest
case class RecentlyMessageResponse(request: RecentlyMessageRequest, result: List[Try[Message]]) extends KafkaActorResponse