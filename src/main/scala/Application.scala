import akka.actor.{Actor, ActorSelection, Props}
import java.io._
import scala.math.min
import scala.util.Random

import Global._

object Application {
  // Constructor
  def props(ip: String, port: Int): Props = Props(new Application(ip, port))

  // Messages
  final case class Publish(topic: String, message: String)
  final case class Subscribe(topic: String)
  final case class Unsubscribe(topic: String)
  final case class PSDeliver(topic: String, message: String)
  final case object GetTopics
  final case object ShowViews
  final case object ShowStats
  final case object ShowNeighbourTopics
  final case object BenchmarkPublish
  final case object BenchmarkSubscribe
  final case object CrashBenchmark
}

class Application(ip: String, port: Int) extends Actor {
  import Application._
  import HyParView._

  // Constants
  val MYSELF: String = s"$ip:$port"

  // State
  var logFile: File = _
  var fileWriter: PrintWriter = _
  var topics: Set[String] = Set()

  // Init
  override def preStart(): Unit = {
    super.preStart()

    val directory: File = new File("messages/")
    if (!directory.exists()) {
      directory.mkdir()
    }
    logFile = new File("messages/" + MYSELF + ".log")
    fileWriter = new PrintWriter(logFile)
  }

  override def postStop(): Unit = {
    super.postStop()
    fileWriter.close()
  }

  // Receive
  override def receive: Receive = {
    case Publish(topic, message) => handlePublish(topic, message)
    case Subscribe(topic) => handleSubscribe(topic)
    case Unsubscribe(topic) => handleUnsubscribe(topic)
    case PSDeliver(topic, message) => handleDeliver(topic, message)
    case GetTopics => handleGetTopics()
    case ShowViews => handleShowViews()
    case ShowNeighbourTopics => handleShowNeighbourTopics()
    case ShowStats => handleShowStats()
    case BenchmarkPublish => handleBenchmarkPublish()
    case BenchmarkSubscribe => handleBenchmarkSubscribe()
    case CrashBenchmark => handleCrashBenchmark()
  }

  /* ---------------------------- Handlers ---------------------------- */

  private def handlePublish(topic: String, message: String): Unit = {
    val log = s"[Published Message] [Topic: $topic] $message\n"
    publishSubscribeActor ! PublishSubscribe.Publish(topic, message)
    logToFile(log)
    println(log)
  }

  private def handleSubscribe(topic: String): Unit = {
    val log = s"[Subscribe] Topic: $topic\n"
    publishSubscribeActor ! PublishSubscribe.Subscribe(topic)
    topics = topics + topic
    logToFile(log)
    println(log)
  }

  private def handleUnsubscribe(topic: String): Unit = {
    val log = s"[Unsubscribe] Topic: $topic\n"
    publishSubscribeActor ! PublishSubscribe.Unsubscribe(topic)
    topics = topics - topic
    logToFile(log)
    println(log)
  }

  private def handleDeliver(topic: String, message: String): Unit = {
    val log = s"[Message Received] [Topic: $topic] $message\n"
    logToFile(log)
    println(log)
  }

  private def handleGetTopics(): Unit = {
    if (topics.nonEmpty) {
      println("############### My Topics ###############")
      topics.foreach(println)
      println()
    } else {
      println("No subscribed topics yet.\n")
    }
  }

  private def handleShowViews(): Unit = {
    hyParViewActor ! LogViews
  }

  private def handleShowNeighbourTopics(): Unit = {
    publishSubscribeActor ! PublishSubscribe.LogNeighboursTopics
  }

  private def handleShowStats(): Unit = {
    publishSubscribeActor ! PublishSubscribe.LogStats
  }

  private def handleBenchmarkSubscribe(): Unit = {
    val topicsPool = (1 to 50).map(i => s"t$i").toSet
    val topicsToSub = getRandomSubset(topicsPool, 5)

    topicsToSub.foreach(t => handleSubscribe(t))
  }

  private def handleBenchmarkPublish(): Unit = {
    val topicsPool = (1 to 50).map(i => s"t$i").toSet
    val topicToPublish = getRandomElement(topicsPool)

    handlePublish(topicToPublish, s"$MYSELF sent this message")
  }

  private def handleCrashBenchmark(): Unit = {
    println("Unsubscribing from previous topics...\n")
    topics.foreach(t => handleUnsubscribe(t))
    Thread.sleep(200)

    val topicsPool = (1 to 5).map(i => s"t$i").toSet

    println("Subscribing to new topics...\n")
    topicsPool.foreach(t => handleSubscribe(t))

    println("Will start to publish soon...\n")
    Thread.sleep(60 * 1000)

    println("Sending a message to each new topic...\n")
    topicsPool.foreach(t => handlePublish(t, s"[$t] $MYSELF sent this message"))
  }

  /* ---------------------------- Procedures and Utils ---------------------------- */

  private def logToFile(log: String): Unit = {
    fileWriter.write(log)
    fileWriter.flush()
  }

  /* ---------------------------- Actors ---------------------------- */

  private def publishSubscribeActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$MYSELF/user/$PUBLISH_SUBSCRIBE_ACTOR_NAME")
  }

  private def hyParViewActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$MYSELF/user/$HYPARVIEW_ACTOR_NAME")
  }

}
