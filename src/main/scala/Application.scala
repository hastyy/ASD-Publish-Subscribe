import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import java.io._

object Application {
  // Constructor
  def props(ip: String, port: Int): Props =
      Props(new Application(ip, port))

  // Messages
  final case class Publish(topic: String, message: String)
  final case class Subscribe(topic: String)
  final case class Unsubscribe(topic: String)
  final case class PSDeliver(topic: String, message: String)
  final case object GetTopics
  final case object Menu
}

class Application(ip: String, port: Int) extends Actor {
  import Application._

  // Constants
  val MYSELF: String = s"$ip:$port"

  // State
  var logFile: File = null
  var fileWriter: PrintWriter = null
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

    case Publish(topic, message) =>
      val log = s"[Published Message] [Topic: $topic] $message\n"
      publishSubscribeActor ! PublishSubscribe.Publish(topic, message)
      fileWriter.write(log)
      fileWriter.flush()
      println(log)

    case Subscribe(topic) =>
      val log = s"[Subscribe] Topic: $topic\n"
      publishSubscribeActor ! PublishSubscribe.Subscribe(topic)
      topics = topics + topic
      fileWriter.write(log)
      fileWriter.flush()
      println(log)

    case Unsubscribe(topic) =>
      val log = s"[Unsubscribe] Topic: $topic\n"
      publishSubscribeActor ! PublishSubscribe.Unsubscribe(topic)
      topics = topics - topic
      fileWriter.write(log)
      fileWriter.flush()
      println(log)

    case PSDeliver(topic, message) =>
      val log = s"[Message Received] [Topic: $topic] $message\n"
      fileWriter.write(log)
      fileWriter.flush()
      println(log)

    case GetTopics =>
      if (topics.nonEmpty) {
        println("############### My Topics ###############")
        topics.foreach(println)
        println()
      } else {
        println("No subscribed topics yet.")
        println()
      }

    case Menu =>
      showHelpMenu()

  }

  private def showHelpMenu() {
    println("\n############### MENU ###############")
    println("Subscribe a topic: SUB topic")
    println("Unsubscribe a topic: UNSUB topic")
    println("Publish a message: PUB topic message")
    println("Get subscribed topics: TOPICS\n\n")
  }

  private def publishSubscribeActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://${Global.SYSTEM_NAME}@$MYSELF/user/${Global.PUBLISH_SUBSCRIBE_ACTOR_NAME}")
  }

}
