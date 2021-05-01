package kafka.cmd.actor

import java.util.Properties
import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingReceive
import kafka.cmd.common.request.DescribeConsumerGroupResponse
import kafka.admin.AdminClient
import kafka.admin.ConsumerGroupCommand.LogEndOffsetResult
import kafka.cmd.common.request.{DescribeConsumerGroupRequest, ListConsumerGroupRequest, ListConsumerGroupResponse}
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by xhuang on 28/04/2017.
  * TODO: 需要把admin功能搞出来, 建立一个AdminActor
  *
  * 获取consumer group的信息
  */
class ConsumerGroupActor(bootstrapServers: String, group: String) extends Actor with ActorLogging {

  var adminClient: AdminClient = null
  var consumer: KafkaConsumer[String, String] = null

  override def receive: Receive = LoggingReceive {
    case r@ListConsumerGroupRequest => sender !
      ListConsumerGroupResponse(r, Try {
        adminClient.listAllGroupsFlattened().map(x => x.groupId)
      })
    case DescribeConsumerGroupRequest(id, targetGroup) => Try {
      if (this.group == targetGroup) {
        println(describeGroup())
        println(s"--------------- describe consumer group $targetGroup finished --------------------")
      }
      else
        throw new IllegalArgumentException(s"Actor ${this.self} only watches $group, but received request for $targetGroup" )
    }
    case request => log.error(s"received unknown request $request")
  }


  override def preStart(): Unit = {
    super.preStart()
    adminClient = createAdminClient()
    consumer = createNewConsumer()
  }


  override def postStop(): Unit = {
    super.postStop()
    adminClient.close()
    consumer.close()
    log.info("ConsumerGroupActor has stopped.")
  }

  private def createAdminClient(): AdminClient = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(props)
  }

  protected def describeGroup(): String = {
    log.info(s"----------------- describing $group -----------------")
    val builder = new StringBuilder
    adminClient.describeConsumerGroup(group) match {
      case None => builder.append(s"Consumer group `${group}` does not exist.")
      case Some(consumerSummaries) =>
        if (consumerSummaries.isEmpty)
          builder.append(s"Consumer group `${group}` is rebalancing.")
        else {
          val consumer = getConsumer()
          builder.append(getDescribeHeader()).append("\n")
          consumerSummaries.foreach { consumerSummary =>
            val topicPartitions = consumerSummary.assignment.map(tp => TopicAndPartition(tp.topic, tp.partition))
            val partitionOffsets = topicPartitions.flatMap { topicPartition =>
              Option(consumer.committed(new TopicPartition(topicPartition.topic, topicPartition.partition))).map { offsetAndMetadata => topicPartition -> offsetAndMetadata.offset
              }
            }.toMap
            describeTopicPartition( topicPartitions, partitionOffsets.get,
              _ => Some(s"${consumerSummary.clientId}_${consumerSummary.clientHost}")
              , builder)
          }
        }
    }
    builder.toString()
  }

  protected def describeTopicPartition(topicPartitions: Seq[TopicAndPartition],
                                       getPartitionOffset: TopicAndPartition => Option[Long],
                                       getOwner: TopicAndPartition => Option[String],
                                       builder: StringBuilder): Unit = {
    topicPartitions
      .sortBy { case topicPartition => topicPartition.partition }
      .foreach { topicPartition =>
        describePartition(topicPartition.topic, topicPartition.partition, getPartitionOffset(topicPartition),
          getOwner(topicPartition), builder)
      }
  }

  protected def getDescribeHeader(): String = {
    "%-30s %-30s %-10s %-15s %-15s %-15s %s".format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "OWNER")
  }

  private def describePartition(topic: String,
                                partition: Int,
                                offsetOpt: Option[Long],
                                ownerOpt: Option[String],
                                builder: StringBuilder): String = {
    def addLeo(logEndOffset: Option[Long]): Unit = {
      val lag = offsetOpt.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))
      builder.append("%-30s %-30s %-10s %-15s %-15s %-15s %s".format(group, topic, partition, offsetOpt.getOrElse("unknown"), logEndOffset.getOrElse("unknown"), lag.getOrElse("unknown"), ownerOpt.getOrElse("none")))
        .append("\n")
    }

    getLogEndOffset(topic, partition) match {
      case LogEndOffsetResult.LogEndOffset(logEndOffset) => addLeo(Some(logEndOffset))
      case LogEndOffsetResult.Unknown => addLeo(None)
      case LogEndOffsetResult.Ignore =>
    }
    builder.append("\n").toString
  }

  protected def getLogEndOffset(topic: String, partition: Int): LogEndOffsetResult = {
    val consumer = getConsumer()
    val topicPartition = new TopicPartition(topic, partition)
    consumer.assign(List(topicPartition).asJava)
    consumer.seekToEnd(List(topicPartition).asJava)
    val logEndOffset = consumer.position(topicPartition)
    LogEndOffsetResult.LogEndOffset(logEndOffset)
  }

  private def getConsumer() = {
    if (consumer == null)
      consumer = createNewConsumer()
    consumer
  }

  private def createNewConsumer(): KafkaConsumer[String, String] = {
    val properties = new Properties()
    val deserializer = (new StringDeserializer).getClass.getName
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

    new KafkaConsumer(properties)
  }
}
