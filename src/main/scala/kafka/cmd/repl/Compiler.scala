package kafka.cmd.repl

import kafka.cmd.common.request.KafkaActorRequest

/**
  * Created by xhuang on 27/04/2017.
  * 在编译过程中出现的异常直接用CompileException抛出来。由使用者来try
  */
trait Compiler {
  def compile(query: String, id: Long): KafkaActorRequest
}
