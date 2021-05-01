package kafka.cmd

import akka.actor.ActorSystem
import kafka.cmd.actor.ConsumerGroupWatcher
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by xhuang on 02/06/2017.
  */
class ConsumerGroupWatcherTest extends FunSuite with BeforeAndAfterAll{
  val bootstrapServers = "slave1.test:9092"
  var actorSystem: ActorSystem = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    actorSystem = ActorSystem("kafka-tools")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    Await.ready(actorSystem.terminate(), 10 seconds)
  }

  test("consumer group watcher should start and exists normally") {
    val watcher = new ConsumerGroupWatcher(actorSystem, "kafka-examples", "slave1.test:2181/kafka01011", bootstrapServers)
    watcher.start()
    Thread.sleep(60 * 1000)
    watcher.stop()
  }
}
