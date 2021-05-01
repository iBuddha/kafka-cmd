package kafka.cmd.repl

import kafka.cmd.common.request._
import kafka.cmd.common.exception.CompileException
import kafka.cmd.common.request.{DescribeConsumerGroupRequest, DescribeTopicRequest, KafkaActorRequest, StopWatchingConsumerGroupRequest, WatchConsumerGroupRequest}

import java.lang

/**
  * Created by xhuang on 27/04/2017.
  */
object DescribeCompiler extends Compiler {
  def compile(s: String, id: Long): KafkaActorRequest =  {
    if(!s.contains("describe"))
      throw new CompileException("not a describe query")
    if(s.split("\\s").length != 3)
      throw new CompileException("must be in form of ' describe topic xxxx' or 'describe group xxxx'")
    val splits = s.split("\\s")
    splits(1) match {
      case "topic" =>  DescribeTopicRequest(id, splits(2))
      case "group" => DescribeConsumerGroupRequest(id, splits(2))
    }
  }
}

object WatchGroupCompiler extends Compiler {
  override def compile(query: String, id: Long): KafkaActorRequest = {
    val splits = query.split("\\s")
    if(splits(0) == "watch") {
      WatchConsumerGroupRequest(id, splits(1))
    } else if(splits(0) == "stop" && splits(1) == "watching") {
      StopWatchingConsumerGroupRequest(id, splits(2))
    } else {
      throw new CompileException("input must be in form of 'watch group xxx' or 'stop watching xxx'")
    }
  }
}
