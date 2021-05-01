package kafka.cmd.common.utils

import java.sql.Timestamp
import kafka.cmd.common.request._
import kafka.cmd.common.TopicPartition
import kafka.cmd.common.request.{DescribeTopicResponse, FindPartitionOffsetRequest, FindPartitionOffsetResponse, FindTopicOffsetResponse, KafkaActorResponse, ListTopicResponse, Message, MessageResponse, OffsetBehindTs, RecentlyMessageResponse, TopicMeta}

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success}

/**
  * Created by xhuang on 25/04/2017.
  */
class Pretty(val response: KafkaActorResponse) {

  import Pretty._

  def pretty = {
    response match {
      case lastestMessage: RecentlyMessageResponse => prettyRecentlyMessageResponse(lastestMessage)
      case findTopicOffset: FindTopicOffsetResponse => prettyFindTopicOffsetResponse(findTopicOffset)
      case findPartitionOffset: FindPartitionOffsetResponse => prettyFindPartitionOffsetResponse(findPartitionOffset)
      case messageResponse: MessageResponse => prettyMessageReponseRequest(messageResponse)
      case describeTopicResponse: DescribeTopicResponse => prettyDescribeTopicResponse(describeTopicResponse)
      case response: ListTopicResponse => prettyListTopicsResponse(response)
    }
  }
}

object Pretty {
  implicit def apply(response: KafkaActorResponse) = new Pretty(response)

  def prettyFindTopicOffsetResponse(response: FindTopicOffsetResponse): String = {
    val FindTopicOffsetResponse(request, partitionOffsets) = response
    implicit val builder = new StringBuilder
    addHeader("Topic", request.topic)
    addHeader("Time", new Timestamp(request.ts).toString)

    ListMap(partitionOffsets.toSeq.sortBy(_._1):_*).foreach {
      case (i, offset) => {
        addItem("Partition", i.toString)
        addItem("Offset", offset.offset.toString)
        addNewLine
      }
    }
    builder.toString
  }

  def prettyRecentlyMessageResponse(response: RecentlyMessageResponse) = {
    val RecentlyMessageResponse(request, result) = response
    result.map(messageTry => {
      messageTry match {
        case Success(message) => Pretty.prettyMessage(message)
        case Failure(e) => e.toString
      }
    }).mkString("\n")
  }

  def prettyListTopicsResponse(response: ListTopicResponse): String = {
    response.result match {
      case Success(topics) => topics.mkString("\n")
      case Failure(e) => e.toString
    }
  }


  def prettyFindPartitionOffsetResponse(response: FindPartitionOffsetResponse): String = {
    val FindPartitionOffsetResponse(FindPartitionOffsetRequest(id, TopicPartition(topic, partiton), ts), OffsetBehindTs(offset, timestamp)) = response
    implicit val builder = new StringBuilder
    addHeader("Topic", topic)
    addHeader("Partition", partiton.toString)
    addHeader("Timestamp", new Timestamp(ts).toString)
    addHeader("Offset", offset.toString)
    builder.toString()
  }

  def prettyMessageReponseRequest(response: MessageResponse): String = {
    response.result.map(m => prettyMessage(m)).recover { case e: Throwable => e.toString }.get
  }

  def prettyDescribeTopicResponse(response: DescribeTopicResponse): String = {
    implicit val builder = new StringBuilder
    val DescribeTopicResponse(request, result) = response
    result match {
      case Failure(e) => e.toString
      case Success(meta) => prettyTopicMeta(meta); builder.toString()
    }
  }

  def prettyTopicMeta(topicMeta: TopicMeta)(implicit builder: StringBuilder) = {
    val TopicMeta(partitionInfo, beginOffsets, endOffsets) = topicMeta
    builder.append("           ------ partition info ----------\n")
    partitionInfo.sortBy(_.partition()).foreach(info => builder.append(info.toString).append("\n"))
    builder.append("           ------  begin offsets ---------\n")
    ListMap(beginOffsets.toSeq.sortBy(_._1.partition):_*).foreach {
      case (tp, offset) => builder.append("Partition-").append(tp.partition).append("\t").append(offset.toString).append("\n")
    }
    builder.append("           ------  end offsets ---------\n")
    ListMap(endOffsets.toSeq.sortBy(_._1.partition):_*).foreach {
      case (tp, offset) => builder.append("Partition-").append(tp.partition).append("\t").append(offset.toString).append("\n")
    }
  }

  //header会单独占一行
  private def addHeader(header: String, value: String, switchLine: Boolean = false)(implicit builder: StringBuilder) = {
    builder.append("[").append(header).append("]: ").append(if (switchLine == true) "\n" else "").append(value).append("\n")
  }

  //item不单独占一行
  private def addItem(key: String, value: String)(implicit builder: StringBuilder) = {
    builder.append("[").append(key).append("]:\t").append(value).append("\t")
  }

  private def addNewLine(implicit builder: StringBuilder) = builder.append("\n")

  implicit def prettyMessage(message: Message) = {
    implicit val builder = new StringBuilder
    addHeader("Partition", message.partition.toString)
    addHeader("Offset", message.offset.toString)
    addHeader("Timestamp", new Timestamp(message.ts).toString)
    addHeader("Key", TextEncoder.encodeUTF8(message.key), true)
    addHeader("Value", TextEncoder.encodeUTF8(message.value), true)
    builder.toString()
  }

}