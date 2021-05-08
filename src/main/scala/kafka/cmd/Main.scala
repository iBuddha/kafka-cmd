package kafka.cmd

import java.util.concurrent.atomic.AtomicLong
import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import kafka.cmd.common.utils.Ago._
import kafka.cmd.common.request.FindTopicOffsetDiffResponse
import kafka.cmd.actor.OffsetLookupActor
import kafka.cmd.common.request.{FindTopicOffsetDiffRequest, FindTopicOffsetDiffResponse}

/**
  * Created by xhuang on 23/04/2017.
  */
object Main extends App {

  implicit private val timeout: Timeout = Timeout(50.seconds)

  val actorSystem = ActorSystem("kafka-tools")
  import actorSystem.dispatcher

  var id = new AtomicLong()
  def newId: Long = id.getAndIncrement()

  val kafkaBrokers = "slave1.test"

  try {
    val offsetLookupActor = actorSystem.actorOf(Props(new OffsetLookupActor()))

    val offsetFuture = (offsetLookupActor ? FindTopicOffsetDiffRequest(newId, "clicks", 24.hoursAgo, 12.hoursAgo))
      .mapTo[FindTopicOffsetDiffResponse]




    offsetFuture.onSuccess {
      case r => println(r)
    }
    offsetFuture.onFailure {
      case e: Exception => e.printStackTrace()
    }
    Await.ready(offsetFuture, 5.seconds)
    offsetFuture.map(respones =>  println(s"all offsets diff is ${respones.sumDiff}"))
  } finally {
    Await.result(actorSystem.terminate(), 5.seconds)
    System.out.println("shutdown")
  }

}
