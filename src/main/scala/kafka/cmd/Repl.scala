package kafka.cmd

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kafka.cmd.actor._
import kafka.cmd.common.request._
import kafka.cmd.common.utils.Pretty._
import kafka.cmd.repl._
import kafka.cmd.actor.{ConsumerGroupActor, ConsumerGroupWatcher, MessageActor, MetadataActor, OffsetLookupActor, SearchMessageActor}
import kafka.cmd.common.exception.CompileException
import kafka.cmd.common.request.{DescribeConsumerGroupRequest, DescribeTopicRequest, KafkaActorRequest, KafkaActorResponse, ListTopicResponse, ListTopicsRequest, MessageRequest, OffsetRequest, RecentlyMessageRequest, RecentlyMessageResponse, StopWatchingConsumerGroupRequest, WatchConsumerGroupRequest}
import kafka.cmd.repl.{ComposedMessageRequestCompiler, DescribeCompiler, MessageLookupCompiler, OffsetLookupCompiler, WatchGroupCompiler}
import org.jline.reader._
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.terminal.TerminalBuilder

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by xhuang on 25/04/2017.
  */
object Repl extends App {
  val config = ConfigFactory.load()
  private val bootstrapServers = getBootstrapServers
  println(s"set bootstrapServers  to $bootstrapServers,\n"
    + "bootstrapServers can be set by modifying application.conf or environment" +
    s" variable KAFKA_BOOTSTRAP_SERVERS")
  private val zkConnect = getZkConnect
  println(s"set bootstrapServers  to $zkConnect,\n"
    + "bootstrapServers can be set by modifying application.conf or environment" +
    s" variable KAFKA_ZK_CONNECT")
  val requestTimeout = 5000L
  val actorSystem = ActorSystem("kafka-tools")
  addShutdownHook()
  val offsetLookupActor = actorSystem.actorOf(Props(new OffsetLookupActor(bootstrapServers)), "offset-actor")
  val messageLookupActor = actorSystem.actorOf(Props(new MessageActor(bootstrapServers, requestTimeout)), "message-actor")
  val metadataActor = actorSystem.actorOf(Props(new MetadataActor(bootstrapServers)), "metadata-actor")
  val latestMessageActor = actorSystem.actorOf(Props(new SearchMessageActor(messageLookupActor.path, offsetLookupActor.path, metadataActor.path)), "currentMessageActor")
  implicit val FutureTimeout = Timeout(20.seconds)
  implicit val context = actorSystem.dispatcher

  // consumer group -> ConsumerGroupWatcher
  var consumerWatches: List[(String, ConsumerGroupWatcher)] = List.empty

  printHelp()

  val topics = getTopis()
  val reader = getReader(topics)

  var id = 0L

  def nextId: Long = {
    id = id + 1
    id
  }

  while (true) {
    try {
      Thread.sleep(100)
      val userInput = reader.readLine("> ").trim.replaceAll("\\p{Cntrl}", "");
      if (!userInput.replaceAll("\\s", "").isEmpty) {
        if (userInput.trim.equalsIgnoreCase("help"))
          printHelp()
        else if (userInput.trim.equalsIgnoreCase("quit"))
          System.exit(0)
        else
          processInput(userInput)
      }
    } catch {
      case e: UserInterruptException => {
        System.err.println("exiting REPL")
        actorSystem.terminate()
        System.exit(0)
      }
      case e: EndOfFileException => {
        stopLastWatch()
      }
      case e: Exception => e.printStackTrace()
        printHelp()
    }
  }

  private def processInput(userInput: String) = {
    import scala.concurrent.duration._
    val future: Option[Future[KafkaActorResponse]] = compile(userInput, nextId) match {
      case Failure(e) => e.printStackTrace(); printHelp(); None
      case Success(request) => request match {
        case request: RecentlyMessageRequest => Some((latestMessageActor ? request).mapTo[RecentlyMessageResponse])
        case request: MessageRequest => Some((messageLookupActor ? request).mapTo[KafkaActorResponse])
        case request: OffsetRequest => Some((offsetLookupActor ? request).mapTo[KafkaActorResponse])
        case request: DescribeTopicRequest => Some((metadataActor ? request).mapTo[KafkaActorResponse])
        case request: ListTopicsRequest => Some((metadataActor ? request).mapTo[ListTopicResponse])
        case WatchConsumerGroupRequest(id, group) => watch(group); None
        case StopWatchingConsumerGroupRequest(id, group) => stopWatch(group); None
        case DescribeConsumerGroupRequest(id, group) => describeGroup(id, group);  None;
      }
    }
    future match {
      case None =>
      case Some(f) =>
        f.onSuccess { case response: KafkaActorResponse => println(response.pretty) }
        f.onFailure { case e: Throwable => println(e.toString) }
        Await.ready(f, 10.seconds)
    }
  }


  private def describeGroup(id: Long, group: String) = {
    val consumerGroupActor = actorSystem.actorOf(Props(new ConsumerGroupActor(bootstrapServers, group)), "consumer-group-actor-" + group)
    consumerGroupActor ! DescribeConsumerGroupRequest(id, group)
    consumerGroupActor ! PoisonPill
  }

  private def watch(group: String) = {
    val watcher = new ConsumerGroupWatcher(actorSystem, group, zkConnect, bootstrapServers)
    consumerWatches = consumerWatches :+ (group, watcher)
    watcher.start()
  }

  private def stopWatch(group: String) = {
    val (groupWatchers, remainingWatches) = consumerWatches.partition(_._1 == group)
    consumerWatches = remainingWatches
    groupWatchers.foreach(_._2.stop())
  }

  private def stopLastWatch() =  {
    if(!consumerWatches.isEmpty) {
      consumerWatches.last._2.stop()
      consumerWatches = consumerWatches.dropRight(1)
    }
  }

  //  private def handleDescribeGroupRequest(request: DescribeConsumerGroupRequest): Option[Future[DescribeConsumerGroupResponse]] = {
  //    val actor = actorSystem.actorOf(Props(new ConsumerGroupActor(bootstrapServers, request.group)), s"consumer-group-actor-${request.group}")
  //  }

  private def printErrorMessage(error: String) = System.err.println(error)

  private def printHelp(): Unit = {
    System.err.println("!!!! 查询offset时，返回的offset是指定时间之后的下一条消息的offset")
    System.err.println("!!!! 使用1@apple，代表topic为apple, partition为1")
    System.err.println("!!!! [[查找一个topic所有分区的offset]]: select offset from my_topic where time = 1h-ago")
    System.err.println("!!!! [[查找一个topic在当前时刻最大offset的下一个offset]]: select offset from 1@my_topic where time = 0s-ago")
    System.err.println("!!!! [[查找一个partition的offset]]: select offset from 1@my_topic where time = 1h-ago")
    System.err.println("!!!! [[根据offset、topic、partition获取消息]]: select message from 1@my_topic where offset = 19000")
    System.err.println("!!!! [[获取一个topic每个分区的最近一条消息]]: select latest message from topic")
    System.err.println("!!!! [[获取一个topic的详细信息]]: describe topic topic_name")
    System.err.println("!!!! [[列出系统所有topic]]: list topics")
    System.err.println("!!!! [[查看消费组的信息]]: describe group consumer_group_id")
    System.err.println("!!!! [[监听一个消费组的情况]]: watch consumer_group_name")
    System.err.println("!!!! [[停止监听一个消费组的情况]]: stop watching consumer_group_name")
    System.err.println("!!!! [[停止监听当前消费组的情况]]: ctrl+D")
    System.err.println("!!!! [[显示帮助信息]]: help")
    System.err.println("!!!! [[退出]]: quit")
  }

  private def compile(query: String, id: Long): Try[KafkaActorRequest] = {
    if (query.trim.split("\\s").mkString(" ").equalsIgnoreCase("list topics"))
      Success(ListTopicsRequest(nextId))
    else
      Try {
        val splits = query.split("\\s")
        if (splits(0).equalsIgnoreCase("describe"))
          DescribeCompiler.compile(query, id)
        else if(splits(0).equalsIgnoreCase("watch") || splits(0).equalsIgnoreCase("stop")){
          WatchGroupCompiler.compile(query = query, id = id)
        }
        else if (splits(1).equalsIgnoreCase("latest"))
          ComposedMessageRequestCompiler.compile(query, id)
        else {
          val compiled: KafkaActorRequest =
            splits(1) match {
              case "offset" => OffsetLookupCompiler.compile(query, id)
              case "message" | "latest" => MessageLookupCompiler.compile(query, id)
              case _ => throw new CompileException("can't parse query " + query)
            }
          compiled
        }
      }
  }

  private def getReader(topics: List[String]): LineReader = {
    //    TerminalFactory.configure("unix")
    val syntaxKeys = List("select", "from", "topic", "message", "offset", "describe", "where", "time", "latest", "ago", "group")
    val completer = new StringsCompleter((syntaxKeys ++ topics).toArray: _*)
    val terminal = TerminalBuilder.builder().system(true).build()
    val reader = LineReaderBuilder.builder().terminal(terminal)
      .appName("kafka-tools")
      .completer(completer)
      .build()
    //    reader.setHistory(new MemoryHistory)
    reader
  }

  private def getTopis(): List[String] = {
    val f = (metadataActor ? ListTopicsRequest(-1)).mapTo[ListTopicResponse].map(_.result).map(_.get)
    Await.result(f, 20.seconds)
  }

  private def addShutdownHook(): Unit = {
    sys.addShutdownHook {
      actorSystem.terminate()
      consumerWatches.foreach {
        case (_, watcher) => watcher.stop()
      }
    }
  }

  def getBootstrapServers = {
    val envProperty = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if(envProperty == null)
      config.getString("kafka.bootstrapServers")
    else
      envProperty
  }

  def getZkConnect = {
    val envProperty = System.getenv("KAFKA_ZK_CONNECT")
    if(envProperty == null)
      config.getString("kafka.zkConnect")
    else envProperty
  }
}
