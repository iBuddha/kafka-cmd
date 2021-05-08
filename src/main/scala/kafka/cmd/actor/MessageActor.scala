package kafka.cmd.actor

import akka.actor.{Actor, ActorLogging}
import kafka.cmd.common.request.Message
import kafka.cmd.common.utils.ConsumerOps._
import kafka.cmd.common.exception.RequestExecutionException
import kafka.cmd.common.request.{Message, MessageRequest, MessageResponse}
import kafka.cmd.common.utils.ConsumerCreator
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

/**
  * Created by xhuang on 25/04/2017.
  */
class MessageActor(timeout: Long) extends Actor with ActorLogging {

  private var consumer = ConsumerCreator.newStandAloneConsumer()

  override def preStart(): Unit = {
    super.preStart()
    consumer = ConsumerCreator.newStandAloneConsumer()
  }

  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
    log.info("MessageActor has stopped.")
  }

  override def receive: Receive = {
    case r: MessageRequest =>
      val originSender = sender()
      Try {
        consumer.message(r.tp, r.offset, timeout)
      } match {
        case Success(record) => originSender ! MessageResponse(r, Success(Message(record.key(), record.value(), record.timestamp(), record.partition(), record.offset())))
        case f: Failure[_] => originSender ! MessageResponse(r, Failure(f.exception))
      }
  }
}
