package kafka.cmd.repl

import kafka.cmd.common.TopicPartition
import kafka.cmd.common.exception.CompileException
import kafka.cmd.common.request.MessageRequest

import scala.util.{Failure, Success, Try}

/**
  * Created by xhuang on 26/04/2017.
  *
  * 形式应该是select message from my_topic where offset=1024
  *
  * 如果在编译中出现问题，对于问题的描述在Right, 编译成功的在Left
  */
object MessageLookupCompiler  extends Compiler{
  private val keys = Set("select", "message", "from", "where")


  def compile(stat: String, id: Long): MessageRequest = {
    val splits = stat.split("\\s")
    if (!(keys -- splits.toSet).isEmpty)
      throw new CompileException("statement should contains " + keys)
    else {
      val fromPosition = splits.indexOf("from")
      val tpComponent = parseTopicPartition(splits(fromPosition + 1))
      val offsetComponent = parseOffset(splits.slice(fromPosition + 3, splits.length).mkString(""))
      val request = for( tp <- tpComponent; offset <- offsetComponent) yield
        MessageRequest(id, tp, offset)
      request match {
        case Success(request) => request
        case Failure(e) =>   throw new CompileException(e.toString)
      }
    }
  }

  private def parseTopicPartition(s: String): Try[TopicPartition] = {
    val splits = s.trim.split("@")
    if (splits.length != 2)
      Failure.apply(new CompileException(s"failed to parse TopicPartition from $s, TopicPartiton must be in form of partition@topic, for example 1@mytopic"))
    else
      Try(TopicPartition(splits(1), splits(0).toInt))
  }

  private def parseOffset(s: String): Try[Long] = {
    val splits = s.split("=")
    if (splits.length == 2 && splits(0).equals("offset"))
      Try(splits(1).toLong)
    else
      Failure(new CompileException(s"offset condition should be in form of \' offset = 111 ', now: $s"))
  }
}
