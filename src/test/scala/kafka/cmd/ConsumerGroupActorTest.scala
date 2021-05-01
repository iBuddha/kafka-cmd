package kafka.cmd

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import kafka.cmd.actor._
import kafka.cmd.common.request._
import kafka.cmd.actor.ConsumerGroupActor
import kafka.cmd.common.request.{DescribeConsumerGroupRequest, DescribeConsumerGroupResponse, ListConsumerGroupRequest, ListConsumerGroupResponse}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


/**
  * Created by xhuang on 25/04/2017.
  */
class ConsumerGroupActorTest extends FunSuite with BeforeAndAfterAll {
  val bootstrapServers = "slave1.test:9092"
  var actorSystem: ActorSystem = null
  var consumerGroupActor: ActorRef = null
  implicit val FutureTimeout = Timeout(20.seconds)
  implicit var context: ExecutionContextExecutor = null
  val requestTimeout = 5000L

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    actorSystem = ActorSystem("kafka-tools")
    consumerGroupActor = actorSystem.actorOf(Props(new ConsumerGroupActor(bootstrapServers, "GID_Bsearch_DispatchInfo")), "consumer-group-actor")
    context = actorSystem.dispatcher
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    Await.ready(actorSystem.terminate(), 10 seconds)
  }

  test("consumer group information should be correct") {

    //  private val bootstrapServers = config.getString("kafka.bootstrapServers")
    Console.println("select offset from Kafka topic or partition")

    val groupsFuture = (consumerGroupActor ? ListConsumerGroupRequest).mapTo[ListConsumerGroupResponse]

    groupsFuture.onSuccess {
      case ListConsumerGroupResponse(request, result) => {
        result match {
          case Success(groups) => groups.foreach(println)
          case Failure(e) => e.printStackTrace()
        }
      }
    }
    groupsFuture.onFailure { case e: Exception => e.printStackTrace() }

    val description = (consumerGroupActor ? DescribeConsumerGroupRequest(1, "GID_Bsearch_DispatchInfo")).mapTo[DescribeConsumerGroupResponse]
    description.onSuccess {
      case DescribeConsumerGroupResponse(request, result) => {
        result match {
          case Success(s) => println(s)
          case Failure(e) => e.printStackTrace()
        }
      }
    }
    description.onFailure { case e: Exception => e.printStackTrace() }

    Await.ready(description, 20.seconds)
  }
}
