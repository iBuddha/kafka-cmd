package kafka.cmd.common.request

import kafka.cmd.common.TopicPartition
import org.apache.kafka.common.PartitionInfo

import scala.util.Try

/**
  * Created by xhuang on 27/04/2017.
  */
trait MetadataRequest extends KafkaActorRequest

trait MetadataResponse extends KafkaActorResponse

case class DescribeTopicRequest(id: Long, topic: String) extends MetadataRequest

case class DescribeTopicResponse(request: DescribeTopicRequest, result: Try[TopicMeta]) extends MetadataResponse

case class TopicMeta(partitionInfo: List[PartitionInfo],
                     beginOffsets: Map[TopicPartition, Long],
                     endOffsets: Map[TopicPartition, Long])

case class TopicExistRequest(id: Long, topic: String) extends KafkaActorRequest
case class TopicExistResponse(request: TopicExistRequest, existed: Boolean)


sealed trait ConsumerGroupRequest extends KafkaActorRequest

case class ListConsumerGroupRequest(id: Long) extends ConsumerGroupRequest
case class ListConsumerGroupResponse(request: ListConsumerGroupRequest.type, groups: Try[List[String]])
case class DescribeConsumerGroupRequest(id: Long, group: String) extends ConsumerGroupRequest
case class DescribeConsumerGroupResponse(request: DescribeConsumerGroupRequest, description: Try[String])
case class WatchConsumerGroupRequest(id: Long, group: String) extends ConsumerGroupRequest
case class StopWatchingConsumerGroupRequest(id: Long, group: String) extends ConsumerGroupRequest
