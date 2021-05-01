package kafka.cmd.actor

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer
import java.util
import java.util.Properties

import akka.actor.ActorSystem
import kafka.common.{OffsetAndMetadata, Topic}
import kafka.coordinator.{GroupMetadataKey, GroupTopicPartition, OffsetKey}
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import akka.event.Logging


/**
  * 通过消费consumer_offsets这个topic来监控一个consumer group的情况。
  * 1. 需要参考Kafka内部确定某个consumer group对应的的consumer offset topic 的分区的方工，找到对应的partition， 然后再指定partition给consumer进行消费
  *
  */
class ConsumerGroupWatcher(system: ActorSystem, groupId: String, zkConnect: String, bootstrapServers: String) {

  //  val LOGGER = LoggerFactory.getLogger(ConsumerGroupWatcher.getClass)
  val loggger = Logging.getLogger(system, ConsumerGroupWatcher.getClass)

  val partitionId = ConsumerGroupWatcher.partitionForGroup(groupId, zkConnect)
  var consumer: Option[KafkaConsumer[Array[Byte], Array[Byte]]] = None
  var consumingThread: Option[Thread] = None

  def start() = {
    consumer = Some(ConsumerGroupWatcher.createKafkaConsumer(bootstrapServers))
    val filter = new SpecifyConsumerGroupFilter(groupId)
    val t = new Thread(new ConsumeRunnable(filter, consumer.get, partitionId))
    t.setName(s"watcher-thread-for-$groupId")
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        loggger.error(e, "error when consuming GroupMetadataTopic")
      }
    })
    consumerThread = Some(t)
    t.start()
    loggger.info(s"start watching $groupId")
  }

  def stop() = {
    loggger.info(s"stop watching $groupId")
    println(s"stop watching $groupId")
    consumerThread.foreach(t => {
      t.interrupt();
      t.join()
    })
  }


  var consumerThread: Option[Thread] = None


  /**
    * 只根据consumer group进行过滤
    */
  class SpecifyConsumerGroupFilter(groupId: String) extends GroupMetadataFilter {
    override def filter(groupTopicPartition: GroupTopicPartition): Boolean = {
      groupTopicPartition.group == groupId
    }

    override def filter(group: String): Boolean = {
      group == groupId
    }
  }


  /**
    * 有可能会只需要观察某个consumer group对于特定topic的消费情况
    */
  trait GroupMetadataFilter {
    def filter(groupTopicPartition: GroupTopicPartition): Boolean

    def filter(groupId: String): Boolean
  }


  class ConsumeRunnable(filter: GroupMetadataFilter, consumer: KafkaConsumer[Array[Byte], Array[Byte]], partitionId: Int) extends Runnable {

    import kafka.cmd.common.utils.GroupMetadataManager.readMessageKey
    import kafka.cmd.common.utils.GroupMetadataManager.readOffsetMessageValue
    import kafka.cmd.common.utils.GroupMetadataManager.readGroupMessageValue

    override def run() = {
      consumer.assign(util.Arrays.asList(new TopicPartition(Topic.GroupMetadataTopicName, partitionId)))
      while (!Thread.currentThread().isInterrupted) {
        val records = consumer.poll(1000)
        val iter = records.iterator()
        while (iter.hasNext) {
          val record = iter.next
          readMessageKey(ByteBuffer.wrap(record.key())) match {
            case OffsetKey(version, key) if filter.filter(key) =>
              val value: OffsetAndMetadata = readOffsetMessageValue(ByteBuffer.wrap(record.value()))
              println(s"$key  $value")
            case GroupMetadataKey(version, key) if filter.filter(key) =>
              val value = readGroupMessageValue(key, ByteBuffer.wrap(record.value()))
              value.allMemberMetadata.foreach {
                mm => println(s"$key  $mm")
              }
          }
        }
      }
    }
  }

}

object ConsumerGroupWatcher {
  def partitionForGroup(groupId: String, zkConnect: String): Int = {
    val zkUtils = ZkUtils(zkConnect, 3000, 3000, false)
    try {
      val partitionAssignment = zkUtils.getPartitionAssignmentForTopics(Seq(Topic.GroupMetadataTopicName))(Topic.GroupMetadataTopicName)
      val metadataPartitionNum = partitionAssignment.size
      if (metadataPartitionNum == 0)
        throw new RuntimeException(Topic.GroupMetadataTopicName + " partition number is zero ")
      Utils.abs(groupId.hashCode) % metadataPartitionNum
    } finally {
      zkUtils.close()
    }
  }

  def createKafkaConsumer(bootstrapServers: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props: Properties = new Properties()
    props.put("group.id", "kafka-tools-consumer-group-watcher")
    props.put("bootstrap.servers", bootstrapServers)
    props.put("exclude.internal.topics", "false")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "latest")
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }
}