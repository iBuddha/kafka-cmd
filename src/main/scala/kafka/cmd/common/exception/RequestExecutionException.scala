package kafka.cmd.common.exception

/**
  * Created by xhuang on 26/04/2017.
  */
class RequestExecutionException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}
