package kafka.cmd.common.utils

import kafka.cmd.common.TopicPartition

/**
  * Created by xhuang on 23/04/2017.
  */
object Converters {
  type ApacheTopicPartition = org.apache.kafka.common.TopicPartition
  implicit def toApacheTopicPartition(topicPartition: TopicPartition): ApacheTopicPartition =
    new ApacheTopicPartition(topicPartition.topic, topicPartition.partition)

  implicit def toScalaTopicPartition(topicPartition: ApacheTopicPartition): TopicPartition =
    TopicPartition(topicPartition.topic(), topicPartition.partition())
}
