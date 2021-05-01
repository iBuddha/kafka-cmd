package kafka.cmd.actor

import akka.actor.Actor
import kafka.cmd.common.utils.Pretty._
import kafka.cmd.common.request.KafkaActorResponse

/**
  *
  * print information to system.out
  *
  */
class PrintActor extends Actor {
  override def receive: Receive = {
    case response: KafkaActorResponse => println(response.pretty)
    case message: String => println(message)
  }
}

