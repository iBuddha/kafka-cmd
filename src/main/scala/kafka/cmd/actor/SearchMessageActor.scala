package kafka.cmd.actor

import akka.actor.{ActorLogging, ActorPath}
import kafka.cmd.common.request._
import akka.pattern.ask
import akka.util.Timeout
import kafka.cmd.common.TopicPartition
import kafka.cmd.common.exception.RequestExecutionException
import kafka.cmd.common.request.{FindTopicOffsetRequest, FindTopicOffsetResponse, MessageRequest, MessageResponse, RecentlyMessageRequest, RecentlyMessageResponse, TopicExistRequest, TopicExistResponse}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by xhuang on 27/04/2017.
  */
class SearchMessageActor(val messageActorPath: ActorPath,
                         val offsetActorPath: ActorPath,
                         val metaActorPath: ActorPath) extends KafkaClientActor with ActorLogging {
  override def receive: Receive = {
    case request@RecentlyMessageRequest(id, topic) => {
      implicit val timeout: Timeout = 5.seconds
      implicit val executionContext = this.context.dispatcher
      val trueSender = sender()
      val topicExists = (context.actorSelection(metaActorPath) ? TopicExistRequest(id, topic)).mapTo[TopicExistResponse].map(_.existed)
      val futureOffsetResponse = topicExists.flatMap(exist => {
        if (exist)
          (context.actorSelection(offsetActorPath) ? FindTopicOffsetRequest(id, topic, System.currentTimeMillis())).mapTo[FindTopicOffsetResponse]
        else
          Future.failed(new RequestExecutionException("no such topic"))
      })
      val messageResponses = futureOffsetResponse.flatMap(response => {
        val messageActorRef = context.actorSelection(messageActorPath)
        val messageRequests = response.partitionOffset.map {
          //minus one to get actually available message offset
          case (partitonId, offset) => MessageRequest(id, TopicPartition(topic, partitonId), offset.offset - 1)
        }
        Future.sequence(messageRequests.map(request => (messageActorRef ? request).mapTo[MessageResponse]))
      })
      messageResponses.onSuccess {
        case messages: Iterable[MessageResponse] => trueSender ! RecentlyMessageResponse(request, messages.map(_.result).toList)
      }
      messageResponses.onFailure {
        case e: Exception => trueSender ! RecentlyMessageResponse(request, List(Failure(e)))
      }
    }
  }
}
