package kafka.cmd.common.request

import kafka.cmd.common.TopicPartition

import scala.util.Try

/**
  * Created by xhuang on 25/04/2017.
  */
case class MessageRequest(id: Long, tp: TopicPartition, offset: Long) extends KafkaActorRequest

//这里的ts是这个Message本身的timestamp
case class MessageResponse(request: MessageRequest, result: Try[Message]) extends KafkaActorResponse
case class Message(key: Array[Byte], value: Array[Byte], ts: Long, partition: Int, offset: Long)
