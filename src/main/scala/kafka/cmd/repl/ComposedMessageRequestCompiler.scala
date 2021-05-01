package kafka.cmd.repl

import kafka.cmd.common.request.KafkaActorRequest
import kafka.cmd.common.exception.CompileException
import kafka.cmd.common.request.{KafkaActorRequest, RecentlyMessageRequest}

/**
  * Created by xhuang on 28/04/2017.
  * select latest message from topic
  */
object ComposedMessageRequestCompiler extends Compiler{
  override def compile(query: String, id: Long): KafkaActorRequest = {
    val splits = query.split("\\s")
    if(splits.length !=5 || !splits.slice(0, 4).mkString(" ").equalsIgnoreCase("select latest message from"))
      throw new CompileException("syntax error")
    else
      RecentlyMessageRequest(id, splits(4))
  }
}
