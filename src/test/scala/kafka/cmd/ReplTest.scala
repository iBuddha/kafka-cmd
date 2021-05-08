package kafka.cmd

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kafka.cmd.actor.OffsetLookupActor
import kafka.cmd.common.request._
import kafka.cmd.repl.DescribeCompiler
import kafka.cmd.actor.{MessageActor, MetadataActor, OffsetLookupActor, SearchMessageActor}
import kafka.cmd.common.utils.Pretty._
import kafka.cmd.common.exception.CompileException
import kafka.cmd.common.request.{KafkaActorRequest, RecentlyMessageRequest, RecentlyMessageResponse}
import kafka.cmd.common.utils.Pretty
import kafka.cmd.repl.{DescribeCompiler, MessageLookupCompiler, OffsetLookupCompiler}

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Try


/**
  * Created by xhuang on 25/04/2017.
  */
object ReplTest extends App {
  val config = ConfigFactory.load()
  private val bootstrapServers = config.getString("kafka.bootstrapServers")
  val requestTimeout = 5000L
  Console.println("select offset from Kafka topic or partition")
  val actorSystem = ActorSystem("kafka-tools")
  val offsetLookupActor = actorSystem.actorOf(Props(new OffsetLookupActor()), "offset-actor")
  val messageLookupActor = actorSystem.actorOf(Props(new MessageActor(requestTimeout)), "message-actor")
  val metadataActor = actorSystem.actorOf(Props(new MetadataActor()), "metadata-actor")
  val currentMessageActor = actorSystem.actorOf(Props(new SearchMessageActor(messageLookupActor.path, offsetLookupActor.path, metadataActor.path)), "currentMessageActor")

  implicit val FutureTimeout = Timeout(10.seconds)
  implicit val context = actorSystem.dispatcher
  printHelp()

  val messageFuture = (currentMessageActor ? RecentlyMessageRequest(1, "clicks")).mapTo[RecentlyMessageResponse]

  messageFuture.onSuccess{
    case RecentlyMessageResponse(request, result) => {
      result.foreach(m => println(Pretty.prettyMessage(m.get)))
    }
  }
  messageFuture.onFailure{case e:Exception => e.printStackTrace()}

  Await.ready(messageFuture, 10.seconds)

  private def printErrorMessage(error: String) = System.err.println(error)

  private def printHelp(): Unit = {
    System.err.println("!!!! 查询offset时，返回的offset是指定时间之后的下一条消息的offset")
    System.err.println("!!!! 使用1@apple，代表topic为apple, partition为1")
    System.err.println("!!!! [[查找一个topic所有分区的offset]]: select offset from my_topic where time = 1h-ago")
    System.err.println("!!!! [[查找一个topic在当前时刻最大offset的下一个offset]]: select offset from 1@my_topic where time = 0s-ago")
    System.err.println("!!!! [[查找一个partition的offset]]: select offset from 1@my_topic where time = 1h-ago")
    System.err.println("!!!! [[根据offset、topic、partition获取消息]]: select message from 1@my_topic where offset = 19000")
    System.err.println("!!!! [[获取一个topic的详细信息]]: describe topic_name")
  }

  private def compile(query: String): Try[KafkaActorRequest] = {
    Try {
      val splits = query.split("\\s")
      if (splits(0).equalsIgnoreCase("describe"))
        DescribeCompiler.compile(query, 0)
      else {
        val compiled: KafkaActorRequest =
          splits(1) match {
            case "offset" => OffsetLookupCompiler.compile(query, 1)
            case "message" => MessageLookupCompiler.compile(query, 1)
            case _ => throw new CompileException("can't recognize query " + query)
          }
        compiled
      }
    }
  }
}
