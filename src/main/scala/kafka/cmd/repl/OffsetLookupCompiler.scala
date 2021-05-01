package kafka.cmd.repl

import kafka.cmd.common.utils.Ago._
import kafka.cmd.common.request.KafkaActorRequest
import kafka.cmd.common.TopicPartition
import kafka.cmd.common.exception.CompileException
import kafka.cmd.common.request.{FindPartitionOffsetRequest, FindTopicOffsetRequest, OffsetRequest}

import scala.util.{Failure, Success, Try}

/**
  * Created by xhuang on 25/04/2017.
  */
object OffsetLookupCompiler  extends Compiler {
  private val keys = Set("select", "offset", "from", "where")

  override def compile(stat: String, id: Long): OffsetRequest = {
    val splits = stat.split("\\s")
    if (!(keys -- splits.toSet).isEmpty)
      throw new CompileException("statement must contains " + keys)
    val desParse = Try(parseDest(splits(3)))
    val timeParse = Try(parseTime(splits.last))
    val offsetRequest = for (des <- desParse; time <- timeParse) yield {
      des match {
        case Left(topic) => FindTopicOffsetRequest(id, topic, time)
        case Right(tp) => FindPartitionOffsetRequest(id, tp, time)
      }
    }
    offsetRequest match {
      case Success(request) => request
      case Failure(e) =>  throw new CompileException(s"failed to compile query: $stat, because: \n.${e.toString}")
    }
  }

  /**
    * 解析查找的对象，结果是一个topic，或者一个分区
    * 要求，如果想要查询某个topic的分区，比如clicks的id为0的分区，应写成  0@clicks这种形式，否则就认为是查询的topic
    *
    * @param s
    * @return
    */
  private def parseDest(s: String): Either[String, TopicPartition] = {
    if (!s.contains("@"))
      Left(s)
    else {
      val splits = s.split("@")
      Try(Right(TopicPartition(splits(1), splits(0).toInt)))
        .getOrElse(Left(s))
    }
  }

  /**
    * 解析查找所依据的时间。可以有以下几种形式：
    * 1. ago:  5h-ago, 3m-ago, 1s-ago
    * 2. 一个准确的时间戳, 也就是一个long
    *
    * @param timeStat
    * @return
    */
  private def parseTime(timeStat: String): Long = {
    try {
      if (timeStat.endsWith("-ago")) {
        val time = timeStat.stripSuffix("-ago").split("=").last
        time.last match {
          case 'h' => time.stripSuffix("h").toInt.hoursAgo
          case 'm' => time.stripSuffix("m").toInt.minutesAgo
          case 's' => time.stripSuffix("s").toInt.secondsAgo
        }
      }
      else
        timeStat.toLong
    } catch {
      case e: Exception => throw new CompileException("failed to parse time from statement")
    }
  }

}
